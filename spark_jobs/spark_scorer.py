###############################################################################
# Real-time Twitter Sentiment Scorer — TRAINING-PARITY FE + INFERENCE LOGIC
# (drop-in for your spark_scorer)
###############################################################################

# ===== imports & env =====
import os, re, json, string, joblib, numpy as np, pandas as pd, logging
from py4j.protocol import Py4JJavaError
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

from pyspark import SparkFiles
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel
from pyspark.ml.feature import MinMaxScalerModel, VectorAssembler, Tokenizer, StopWordsRemover, NGram
from pyspark.sql.functions import udf, pandas_udf
from pyspark.ml.functions import vector_to_array

# ===== logger =====
try:
    logger  # noqa
except NameError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("scorer")

# ===== paths / config =====
HERE      = os.path.dirname(__file__)
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "tweets")
OUT_TOPIC = os.getenv("OUT_TOPIC", "twitter_sentiment")
STARTING  = os.getenv("STARTING_OFFSETS", "earliest")
CKPT_BASE = os.getenv("CKPT_BASE", "/home/ubuntu/app/sentiment_realtime_project/checkpoints")

ART_DIR   = os.getenv("BERTWEET_ART_DIR", os.path.join(BASE_DIR, "bertweet_artifacts"))

MONGO_URI_BASE = os.getenv("MONGO_URI_BASE", "").rstrip("/")
MONGO_DB       = os.getenv("MONGO_DB", "twitter_rt")
MONGO_COLL     = os.getenv("MONGO_COLL", "scored_tweets")

# shipped file names (provided via --files)
ONNX_PATH       = os.getenv("PN_ONNX_PATH", "pn_bertweet.onnx")
GATE_PKL        = os.getenv("GATE_PKL", "gate_xgb.pkl")
THRESHOLDS_JSON = os.getenv("THRESHOLDS_JSON", "thresholds_and_config.json")

HF_DIR    = os.getenv("HF_DIR", os.path.join(ART_DIR, "hf"))

# models (local dirs, not archives)
TFIDF_PATH  = os.getenv("TFIDF_MODEL_PATH",  "tfidf_model_shared")
SCALER_PATH = os.getenv("SCALER_MODEL_PATH", "minmax_scaler_model_shared")

# ===== Spark =====
spark = (
    SparkSession.builder
    .appName("TwitterSentimentScorer-HierONNX")
    .getOrCreate()
)
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "64")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("WARN")

# ===== shipped-files resolver =====
def _resolve_local(name_or_path: str) -> str:
    base = os.path.basename(name_or_path)
    local = SparkFiles.get(base)
    return local if os.path.exists(local) else name_or_path

PN_PATH_RESOLVED   = _resolve_local(ONNX_PATH)
GATE_PATH_RESOLVED = _resolve_local(GATE_PKL)
THRESHOLDS_PATH    = _resolve_local(THRESHOLDS_JSON)

# ---- load gate & meta ----
gate = joblib.load(GATE_PATH_RESOLVED)
try:
    with open(THRESHOLDS_PATH) as f:
        meta = json.load(f)
except FileNotFoundError:
    meta = {}

# thresholds (from meta first, else env, else defaults)
gate_t = float(meta.get("gate_t", os.getenv("GATE_T", meta.get("t_neutral", 0.725))))
pn_t   = float(meta.get("pn_t",   os.getenv("PN_T",   0.45)))
# neutral class id from meta (training saved gate_neutral_class: 1)
GATE_NEU_CLASS = int(meta.get("gate_neutral_class",
                      os.getenv("GATE_NEU_CLASS", os.getenv("gate_neutral_class", "1"))))

BERT_BATCH = int(os.getenv("BERT_BATCH", "16"))
MAX_LEN    = int(os.getenv("MAX_LEN", str(meta.get("max_len", 224))))

logger.info(
    "Loaded gate; gate_t=%.3f pn_t=%.3f neutral_class=%s thresholds_json=%s",
    gate_t, pn_t, GATE_NEU_CLASS, THRESHOLDS_PATH
)

# ===== load artifacts that need SparkContext =====
tfidf_model  = PipelineModel.load(TFIDF_PATH)
scaler_model = MinMaxScalerModel.load(SCALER_PATH)

# ===== sentence embeddings (CPU) =====
from sentence_transformers import SentenceTransformer
EMBED_MODEL_NAME = os.getenv("SENTENCE_MODEL_NAME", "all-MiniLM-L6-v2")
EMB_DIM = int(os.getenv("EMB_DIM", "384"))  # default/fallback
_embedder = None          # None = not loaded, False = permanently disabled

def _lazy_embedder():
    """Load sentence transformer once per worker (fail-soft)."""
    global _embedder, EMB_DIM
    if _embedder is None:
        try:
            _embedder = SentenceTransformer(EMBED_MODEL_NAME)
            try:
                EMB_DIM = int(_embedder.get_sentence_embedding_dimension())
            except Exception:
                pass
        except Exception:
            _embedder = False
    return _embedder

# ===== PN ONNX (lazy per-executor) =====
import onnxruntime as ort
from transformers import AutoTokenizer

_onnx_sess = None
_onnx_tok  = None

def _lazy_onnx():
    global _onnx_sess, _onnx_tok
    if _onnx_sess is None:
        sess_opts = ort.SessionOptions()
        providers = ["CPUExecutionProvider"]
        _onnx_sess = ort.InferenceSession(
            PN_PATH_RESOLVED, sess_options=sess_opts, providers=providers
        )
        # prefer local HF bundle (offline)
        tok_dir = HF_DIR if os.path.isdir(HF_DIR) else "vinai/bertweet-base"
        _onnx_tok = AutoTokenizer.from_pretrained(
            tok_dir, local_files_only=True, use_fast=False, normalization=True
        )
    return _onnx_sess, _onnx_tok

# --- language detection helpers ---
ISO2_TO_3 = {
    "en":"eng","ur":"urd","ar":"ara","tr":"tur","es":"spa","fr":"fra","de":"deu","it":"ita",
    "pt":"por","hi":"hin","fa":"fas","zh":"zho","ja":"jpn","ko":"kor","ru":"rus","uk":"ukr",
    "pl":"pol","nl":"nld","sv":"swe","fi":"fin","id":"ind","ms":"msa","vi":"vie","th":"tha",
    "bn":"ben","ta":"tam","te":"tel","gu":"guj","mr":"mar","pa":"pan","he":"heb","el":"ell",
    "ro":"ron","cs":"ces","sk":"slk","sr":"srp","bg":"bul","hu":"hun"
}

def _to_iso3_py(code:str|None)->str:
    if not code: return "und"
    c = code.strip().lower()
    if len(c)==3:  # already iso-3?
        return c
    return ISO2_TO_3.get(c, "und")

try:
    import langid
    _have_langid = True
except Exception:
    _have_langid = False

@udf(T.StringType())
def detect_lang_udf(s):
    txt = (s or "").strip()
    if len(txt) < 6:
        # super short → guess english if mostly ascii letters/spaces, else und
        import re, string
        if txt and all((ch in string.printable) for ch in txt):
            return "eng"
        return "und"
    if _have_langid:
        try:
            code2 = langid.classify(txt)[0]  # e.g., 'en'
            return _to_iso3_py(code2)
        except Exception:
            pass
    # lightweight heuristic fallback
    try:
        ascii_ratio = sum(ch.isascii() for ch in txt)/max(len(txt),1)
        return "eng" if ascii_ratio > 0.95 else "und"
    except Exception:
        return "und"

@udf(T.StringType())
def to_iso3(code):
    return _to_iso3_py(code)

# ===== cleaning helpers (TRAINING-PARITY) =====
_word_re = re.compile(r"\s+")
def _clean_py(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower()
    s = re.sub(r"http\S+|www\S+", " ", s)   # urls
    s = re.sub(r"@\w+", " ", s)             # mentions
    s = re.sub(r"#", " ", s)                # keep hashtag token, drop only '#'
    s = re.sub(r"[^\x00-\x7f]", " ", s)     # non-ascii
    s = re.sub(r"(.)\1{2,}", " ", s)        # repeated chars → tame
    s = re.sub(f"[{re.escape(string.punctuation)}]", " ", s)  # punct
    s = _word_re.sub(" ", s).strip()
    return s

@udf(T.StringType())
def clean_text_udf(s): return _clean_py(s)

# ===== constants (same as training) =====
SENTIMENT_KW = F.array(*[F.lit(x) for x in
    ["love","hate","amazing","awful","great","bad","good","terrible","worst","excellent"]])
NEG_WORDS    = F.array(*[F.lit(x) for x in
    ["not","no","never","none","nobody","isn't","wasn't","aren't","don't","didn't","won't","can't"]])

# ===== VADER (broadcast) + training-style bucket =====
from nltk.sentiment.vader import SentimentIntensityAnalyzer
_vader = SentimentIntensityAnalyzer()
bc_vader = spark.sparkContext.broadcast(_vader)

@udf(T.DoubleType())
def vader_compound(txt):
    try:
        return float(bc_vader.value.polarity_scores(txt or "").get("compound", 0.0))
    except Exception:
        return 0.0

def add_vader(df):
    df = df.withColumn("vader_score", vader_compound("clean_text"))
    df = df.withColumn(
        "vader_bucket",
        F.when(F.col("vader_score") <= -0.4, F.lit(0))
         .when(F.col("vader_score") >= 0.4, F.lit(1))
         .otherwise(F.lit(2)).cast("int")
    )
    return df

# ===== light lexicons (training-style) + opinion_lexicon (fail-soft) =====
positive_lexicon = {"good","great","excellent","amazing","love","nice","happy","best"}
negative_lexicon = {"bad","terrible","awful","hate","worst","sad","horrible","angry"}

pos_bv = spark.sparkContext.broadcast(positive_lexicon)
neg_bv = spark.sparkContext.broadcast(negative_lexicon)

@udf(T.IntegerType())
def count_pos(text): return sum(1 for w in (text or "").split() if w in pos_bv.value)
@udf(T.IntegerType())
def count_neg(text): return sum(1 for w in (text or "").split() if w in neg_bv.value)

try:
    from nltk.corpus import opinion_lexicon
    ext_pos_bv = spark.sparkContext.broadcast(set(opinion_lexicon.positive()))
    ext_neg_bv = spark.sparkContext.broadcast(set(opinion_lexicon.negative()))
except Exception:
    ext_pos_bv = spark.sparkContext.broadcast(set())
    ext_neg_bv = spark.sparkContext.broadcast(set())

@udf(T.IntegerType())
def count_ext_pos(text): return sum(1 for w in (text or "").split() if w in ext_pos_bv.value)
@udf(T.IntegerType())
def count_ext_neg(text): return sum(1 for w in (text or "").split() if w in ext_neg_bv.value)

# ===== time features (training-style) =====
def add_time_cols(df):
    cands = []

    # Prefer already-parsed event_time if present
    if "event_time" in df.columns:
        cands.append(F.col("event_time").cast("timestamp"))

    # Twitter string date (rare in your stream)
    if "date" in df.columns:
        cands.append(F.to_timestamp(F.col("date"), "EEE MMM dd HH:mm:ss Z yyyy"))

    # created_at might be epoch (ms or sec) or string — handle both
    if "created_at" in df.columns:
        dt = df.schema["created_at"].dataType
        if isinstance(dt, T.LongType):
            # assume milliseconds; adjust if your producer sends seconds
            cands.append(F.to_timestamp(F.from_unixtime(F.col("created_at")/1000.0)))
        else:
            cands.append(F.to_timestamp(F.col("created_at")))

    # generic 'timestamp' column if exists
    if "timestamp" in df.columns:
        cands.append(F.col("timestamp").cast("timestamp"))

    # your 'ts' is epoch-ms (as per upstream usage)
    if "ts" in df.columns:
        cands.append(F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))

    # fold coalesces only over existing exprs
    ts = None
    for expr in cands:
        ts = expr if ts is None else F.coalesce(ts, expr)
    if ts is None:
        ts = F.current_timestamp()

    df = df.withColumn("date_ts", ts)
    df = (df
          .withColumn("hour", F.hour("date_ts"))
          .withColumn("day_of_week", F.dayofweek("date_ts"))
          .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0))
    )
    return df


# ===== syntactic/semantic flags & counts (training-parity) =====
def add_flags_and_counts(df):
    df = (df
      .withColumn("text_lower", F.lower("text"))
      .withColumn("tokens", F.split("clean_text", r"\s+"))
      .withColumn("text_length", F.length("clean_text"))
      .withColumn("word_count", F.when(F.col("clean_text")=="" , 0)
                                  .otherwise(F.size(F.split("clean_text"," "))))
      .withColumn("char_density", F.when(F.col("word_count")==0, F.lit(0.0))
                                   .otherwise(F.col("text_length")/F.col("word_count")))
      .withColumn("char_density", F.when(F.col("char_density")>20.0, 20.0).otherwise(F.col("char_density")))
      .withColumn("has_mentions", (F.instr("text", "@") > 0).cast("int"))
      .withColumn("has_hashtags", (F.instr("text", "#") > 0).cast("int"))
      .withColumn("has_links", ((F.instr("text_lower","http")>0)|(F.instr("text_lower","www")>0)).cast("int"))
      .withColumn("is_question", F.when(F.col("text").rlike(r"\?$"), 1).otherwise(0))
      .withColumn("sentiment_keyword_count",
                  F.size(F.array_intersect(F.col("tokens"), SENTIMENT_KW)))
      .withColumn("negation_count",
                  F.size(F.array_intersect(F.col("tokens"), NEG_WORDS)))
      .withColumn("capital_word_count", F.size(F.expr(
          "filter(split(text,'\\s+'), x -> x = upper(x) AND length(x) >= 2)"
      )))
      .withColumn("emoji_count", F.length(F.regexp_replace("text", r"[\w\s,]", "")))
      .withColumn("positive_lexicon_count", count_pos("clean_text"))
      .withColumn("negative_lexicon_count", count_neg("clean_text"))
      .withColumn("extended_positive_lexicon_count", count_ext_pos("clean_text"))
      .withColumn("extended_negative_lexicon_count", count_ext_neg("clean_text"))
      .withColumn("punctuation_count",
                  F.length(F.regexp_replace(F.col("text"), f"[^{re.escape(string.punctuation)}]", "")))
    )
    # training-style: cap emojis to tame skew
    df = df.withColumn("emoji_count", F.least(F.col("emoji_count"), F.lit(5)))
    return df

# ===== log1p (same candidate list used in training) =====
LOG1P_COLS = [
    "text_length","word_count","char_density",
    "sentiment_keyword_count","negation_count",
    "emoji_count","capital_word_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
]
def apply_log1p(df):
    for c in LOG1P_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.log1p(F.col(c).cast("double")))
    return df

# ===== numeric features order — MUST MATCH TRAINING =====
NUMERIC_FEATURES = [
    "hour","day_of_week","is_weekend","text_length","word_count","char_density",
    "has_mentions","has_hashtags","has_links","is_question",
    "sentiment_keyword_count","negation_count",
    "emoji_count","capital_word_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
    "vader_score","vader_bucket"
]

def add_scaled_numeric(df):
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="numeric_raw")
    df = assembler.transform(df)
    df = scaler_model.transform(df)   # pre-fitted MinMaxScaler from training
    df = df.withColumn("numeric_arr", vector_to_array("numeric_vec"))
    return df

# ===== TF-IDF pipeline parity (Tokenizer → StopWords → NGram(2) → merge) =====
merge_udf = F.udf(lambda uni, bi: (uni or []) + (bi or []),
                  T.ArrayType(T.StringType()))
_tok  = Tokenizer(inputCol="clean_text", outputCol="words")
_rem  = StopWordsRemover(inputCol="words", outputCol="words_clean")
_ng   = NGram(n=2, inputCol="words_clean", outputCol="bigrams")

def add_tfidf_cols(df):
    df = _tok.transform(df)
    df = _rem.transform(df)
    df = _ng.transform(df)
    df = df.withColumn("merged_ngrams", merge_udf(F.col("words_clean"), F.col("bigrams")))
    df = tfidf_model.transform(df)               # expects merged_ngrams
    df = df.withColumn("tfidf_arr", vector_to_array("tfidf_vec"))
    return df

# -------------------- PREDICTION UDF (cleaned, parity) --------------------
predict_schema = T.StructType([
    T.StructField("pred_label", T.IntegerType(), nullable=True),  # 0=neg, 1=pos (only for routed rows)
    T.StructField("p_neu",      T.DoubleType(),  nullable=True),  # gate neutral prob (always)
    T.StructField("p_pos",      T.DoubleType(),  nullable=True),  # PN positive prob (0.0 if not routed)
    T.StructField("err",        T.StringType(),  nullable=True),  # per-row error ("" if ok)
])

@pandas_udf(predict_schema)
def predict_udf(
    text: pd.Series,
    clean_texts: pd.Series,
    tfidf_arr: pd.Series,
    numeric_arr: pd.Series
) -> pd.DataFrame:
    N = len(clean_texts)

    # Pre-alloc
    yhat  = np.zeros(N, dtype=np.int32)
    p_neu = np.zeros(N, dtype=np.float32)
    p_pos = np.zeros(N, dtype=np.float32)
    ids_err = np.array([""]*N, dtype=object)

    if N == 0:
        return pd.DataFrame({"pred_label": yhat, "p_neu": p_neu, "p_pos": p_pos, "err": ids_err})

    # ----- build gate features -----
    # tfidf
    try:
        tf_list = tfidf_arr.to_list()
        tf_mat = np.vstack(tf_list).astype(np.float32, copy=False) if len(tf_list) else np.zeros((N, 0), np.float32)
    except Exception:
        tf_mat = np.zeros((N, 0), np.float32)

    # numeric
    try:
        nv_list = numeric_arr.to_list()
        nv_mat = np.vstack(nv_list).astype(np.float32, copy=False) if len(nv_list) else np.zeros((N, 0), np.float32)
    except Exception:
        nv_mat = np.zeros((N, 0), np.float32)

    # embeddings (normalize_embeddings=True to match training)
    txts = clean_texts.fillna("").tolist()
    try:
        emb = _lazy_embedder()
        if emb is False:
            raise RuntimeError("embedder_disabled")
        emb_mat = emb.encode(
            txts,
            batch_size=64,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True
        ).astype(np.float32, copy=False)
    except Exception as e:
        emb_mat = np.zeros((N, EMB_DIM), dtype=np.float32)
        ids_err = np.where(ids_err == "", f"embed_fail:{type(e).__name__}", ids_err)

    # full feature matrix (tolerate empty tf/nv)
    try:
        X = np.hstack([tf_mat, nv_mat, emb_mat]).astype(np.float32, copy=False)
    except Exception:
        try:
            X = np.hstack([nv_mat, emb_mat]).astype(np.float32, copy=False)
        except Exception:
            X = emb_mat  # never fail the batch

    # ----- gate proba -> p_neu -----
    need_idx = np.array([], dtype=np.int64)
    try:
        proba = gate.predict_proba(X)  # (N, C)
        classes = list(getattr(gate, "classes_", []))

        if classes:
            try:
                idx_neu = classes.index(GATE_NEU_CLASS)
            except ValueError:
                idx_neu = 1 if len(classes) > 1 else 0
            idx_non = 1 - idx_neu if len(classes) == 2 else (0 if idx_neu != 0 else 1)
        else:
            idx_neu = 1 if proba.shape[1] > 1 else 0
            idx_non = 1 - idx_neu if proba.shape[1] == 2 else idx_neu

        # training saves proba(neutral) as positive label -> take idx_neu
        p_neu = proba[:, idx_neu].astype(np.float32, copy=False)
        need_idx = np.where(p_neu < float(gate_t))[0]
    except Exception as e:
        ids_err = np.where(ids_err == "", f"gate_fail:{type(e).__name__}", ids_err)
        p_neu[:] = 1.0
        need_idx = np.array([], dtype=np.int64)

    # ----- PN (ONNX) only for routed rows -----
    if need_idx.size > 0:
        try:
            sess, tok = _lazy_onnx()
            pos_t = float(pn_t)
            for start in range(0, need_idx.size, BERT_BATCH):
                sl = need_idx[start:start + BERT_BATCH]
                batch_txts = [txts[i] for i in sl]

                enc = tok(
                    batch_txts,
                    return_tensors="np",
                    truncation=True,
                    max_length=MAX_LEN,
                    padding=True
                )

                logits = sess.run(None, {
                    "input_ids": enc["input_ids"],
                    "attention_mask": enc["attention_mask"]
                })[0]  # (B, 2)

                # numerically-stable softmax
                logits = logits - logits.max(axis=1, keepdims=True)
                exps = np.exp(logits, dtype=np.float32)
                probs = exps / exps.sum(axis=1, keepdims=True)

                p_batch = probs[:, 1].astype(np.float32, copy=False)  # P(pos)
                p_pos[sl] = p_batch
                yhat[sl] = (p_batch >= pos_t).astype(np.int32)
        except Exception as e:
            p_pos[need_idx] = 0.0
            yhat[need_idx] = 0
            for i in need_idx:
                if ids_err[i] == "":
                    ids_err[i] = f"onnx_fail:{type(e).__name__}"

    # return DataFrame in fixed order/dtypes
    return pd.DataFrame({
        "pred_label": yhat.astype(np.int32),
        "p_neu":      p_neu.astype(np.float64),
        "p_pos":      p_pos.astype(np.float64),
        "err":        ids_err
    })

def build_features_then_score(src_df):
    """
    src_df must contain at least: 'text' (string) and ideally a timestamp column:
      one of ['date' (Twitter format), 'created_at', 'timestamp', 'ts'].
    Returns src_df + ['clean_text','numeric_vec','tfidf_vec','pred_label','p_neu','p_pos','err'].
    """
    df = src_df.withColumn("text", F.col("text").cast("string"))
    df = df.withColumn("clean_text", clean_text_udf("text"))

    # ensure 'lang' column exists (just in case)
    if "lang" not in df.columns:
        df = df.withColumn("lang", F.lit(None).cast(T.StringType()))

    # ---- feature engineering (training parity) ----
    df = add_time_cols(df)
    df = add_flags_and_counts(df)
    df = add_vader(df)
    df = apply_log1p(df)
    df = add_scaled_numeric(df)   # adds numeric_vec + numeric_arr
    df = add_tfidf_cols(df)       # adds tfidf_vec  + tfidf_arr

    # ---- predictions (gate + PN via predict_udf) ----
    df = (
        df.withColumn(
            "pred_struct",
            predict_udf(
                F.col("text"),
                F.col("clean_text"),
                F.col("tfidf_arr"),
                F.col("numeric_arr"),
            )
        )
        .withColumns({
            "pred_label": F.col("pred_struct.pred_label"),
            "p_neu":      F.col("pred_struct.p_neu"),
            "p_pos":      F.col("pred_struct.p_pos"),
            "err":        F.col("pred_struct.err"),
        })
        .drop("pred_struct")
    )

    # ---- language fill (fallback only when null/empty/und & text long enough) ----
    df = df.withColumn("lang_pred", detect_lang_udf(F.col("clean_text")))
    df = df.withColumn(
        "lang",
        F.when(
            (F.col("lang").isNull() | (F.col("lang") == "") | (F.lower(F.col("lang")) == "und")) &
            (F.length(F.col("clean_text")) >= F.lit(5)),  # skip very short texts
            F.col("lang_pred")
        ).otherwise(F.col("lang"))
    )
    # map 2-letter -> 3-letter (en->eng, ur->urd, etc.)
    df = df.withColumn("lang", to_iso3(F.lower(F.col("lang"))))
    df = df.drop("lang_pred")

    return df


# ==================== how to use in your stream ====================

# --------------------------- Read from Kafka ---------------------------------
tweet_schema = T.StructType([
    T.StructField("ids", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("ts", T.LongType()),
    T.StructField("created_at", T.LongType()),
    T.StructField("category", T.StringType()),
    T.StructField("hasLink", T.BooleanType()),
    T.StructField("origin", T.StringType()),
    T.StructField("lang", T.StringType()),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", IN_TOPIC)
    .option("startingOffsets", STARTING)
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS value")
    .select(F.from_json("value", tweet_schema, {"mode": "PERMISSIVE"}).alias("data"))
    .where(F.col("data").isNotNull())
    .where(F.col("data.ids").isNotNull() & F.col("data.text").isNotNull())
    .where(F.length(F.col("data.text")) >= 3)
    .select("data.*")
)

dedup_in = (
    parsed_df
    .withColumn("event_time",
                F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))
    .withColumn("_id", F.col("ids").cast(T.StringType()))
    .withWatermark("event_time", "1 day")
    .dropDuplicates(["ids"])
)

base_for_pred = (
    dedup_in
    .select("ids","text","ts","event_time","category","hasLink","origin","lang")
    .withColumn("created_at_ts", F.col("event_time").cast(T.TimestampType()))
    .transform(build_features_then_score)     # <-- ye FE + arrays add karega
    .withColumn("tf_size", F.size("tfidf_arr"))
    .withColumn("nv_size", F.size("numeric_arr"))
)

# ===== prediction + final shaping ============================================
out = (
    base_for_pred
    .select(
        "ids","text","clean_text","ts","event_time","category","hasLink","origin","lang",
        "created_at_ts","tf_size","nv_size",
        "pred_label","p_neu","p_pos","err"     # <-- reuse from build_features_then_score
    )
    .withColumn(
        "created_at",
        F.coalesce(F.col("created_at_ts"),
                   F.col("event_time").cast(T.TimestampType()),
                   F.current_timestamp())
    )
    .withColumn("processed_at", F.current_timestamp())
)

# ---- final 3-class label + consistent column names ----
scored_df = (
    out
    .withColumn("tweet_id", F.col("ids"))                                 # <- consistent id for sinks
    .withColumn("need_pn", (F.col("p_neu") < F.lit(float(gate_t))))
    .withColumn(                                                         # <- 3-class hard label
        "final_label",
        F.when(F.col("p_neu") >= F.lit(float(gate_t)), F.lit(2))         # 2 = neutral
         .otherwise(F.col("pred_label"))                                 # 0=neg, 1=pos
    )
    .withColumn("sentiment_label", F.col("final_label").cast("int"))
    .withColumn(                                                         # <- human-readable
        "sentiment",
        F.when(F.col("sentiment_label") == 2, F.lit("neutral"))
         .when(F.col("sentiment_label") == 1, F.lit("positive"))
         .otherwise(F.lit("negative"))
    )
    .withColumn("sentiment_text", F.col("sentiment"))                    # <- keep if downstream expects this
    .withColumn("need_pn_dbg", F.col("need_pn").cast("int"))
    .withColumn("gate_t", F.lit(float(gate_t)))
    .withColumn("pn_t",   F.lit(float(pn_t)))
    .withColumn("origin", F.coalesce(F.col("origin"), F.lit("live")))
    .withColumn("source", F.lit("scorer"))
    .withColumn("type",   F.lit("tweet"))
    .withColumn("lang",   F.coalesce(F.col("lang"), F.lit("und")))
)

print("\n[scorer] ==== SCHEMAS & COLS ====", flush=True)
print("[scorer] scored_df schema:", flush=True); scored_df.printSchema()
print("[scorer] ========================\n", flush=True)
# ======================= foreachBatch sinks (SAFE) ============================
unified_ckpt = f"{CKPT_BASE}/unified_v1"
# at top-level imports:
from pyspark import StorageLevel
from py4j.protocol import Py4JJavaError
import traceback

MONGO_COLL_METRICS = os.environ.get("MONGO_COLL_METRICS", f"{MONGO_COLL}_metrics")

def write_to_sinks(batch_df, epoch_id: int):
    micro = None
    err_msgs = []
    try:
        micro = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        rows = micro.count()
        print(f"[foreachBatch] epoch={epoch_id} rows={rows}", flush=True)
        if rows == 0:
            return  # nothing to do

        # ---------- METRICS ROW ----------
        has_sizes = ("tf_size" in micro.columns) and ("nv_size" in micro.columns)
        gate_t_lit = F.lit(float(gate_t))

        aggs = [F.count(F.lit(1)).alias("rows")]
        if has_sizes:
            bad_cond = (F.col("tf_size") != F.lit(3000)) | (F.col("nv_size") != F.lit(21))
            aggs += [
                F.sum(F.when(bad_cond, 1).otherwise(0)).alias("bad_size_rows"),
                F.min("tf_size").alias("tf_min"), F.max("tf_size").alias("tf_max"),
                F.min("nv_size").alias("nv_min"), F.max("nv_size").alias("nv_max"),
            ]
        if "p_neu" in micro.columns:
            aggs += [
                F.avg(F.when(F.col("p_neu") >= gate_t_lit, 1.0).otherwise(0.0)).alias("frac_neutral_gate"),
                F.expr("percentile_approx(p_neu, array(0.01,0.1,0.5,0.9,0.99))").alias("p_neu_pctls")
            ]
        metrics_df = (micro.agg(*aggs)
                      .withColumn("batch_id", F.lit(int(epoch_id)))
                      .withColumn("ts", F.current_timestamp()))

        # ---------- Kafka OUT ----------
        try:
            kafka_df = micro.selectExpr(
                "CAST(ids AS STRING) AS key",
                "to_json(named_struct("
                " 'tweet_id', ids, "
                " 'text', text, "
                " 'clean_text', clean_text, "
                " 'sentiment', sentiment, "
                " 'sentiment_label', sentiment_label, "
                " 'p_neu', p_neu, "
                " 'p_pos', p_pos, "
                " 'created_at', created_at, "
                " 'processed_at', processed_at, "
                " 'type', type, "
                " 'category', category, "
                " 'hasLink', hasLink, "
                " 'origin', origin, "
                " 'lang', lang, "
                " 'source', source "
                ")) AS value"
            )
            (kafka_df.write
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP)
                .option("topic", OUT_TOPIC)
                .save())
        except (Py4JJavaError, Exception) as e:
            msg = f"kafka_write:{type(e).__name__}:{str(e)[:200]}"
            print("[foreachBatch] ERROR", msg, flush=True)
            err_msgs.append(msg)

        # ---------- Mongo LATEST ----------
        try:
            latest = (micro
                .withColumn("created_day", F.to_date("created_at"))
                .withColumn("processed_day", F.to_date("processed_at"))
                .withColumn("_id", F.col("ids"))
                .select("_id", "ids", F.col("ids").alias("tweet_id"),
                        "text", "clean_text",
                        "sentiment", "sentiment_label", "p_neu", "p_pos",
                        "created_at", "created_day", "processed_day",
                        "category", "type", "origin", "lang", "source", "hasLink")
                .where(F.col("_id").isNotNull() & (F.length("_id") > 0)))
            (latest.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
                .option("spark.mongodb.write.database", MONGO_DB)
                .option("spark.mongodb.write.collection", MONGO_COLL)
                .option("spark.mongodb.write.operationType", "replace")
                .mode("append")
                .save())
        except (Py4JJavaError, Exception) as e:
            msg = f"mongo_latest:{type(e).__name__}:{str(e)[:200]}"
            print("[foreachBatch] ERROR", msg, flush=True)
            err_msgs.append(msg)

        # ---------- Mongo HISTORY ----------
        try:
            mongo_coll_hist = os.environ.get("MONGO_COLL_HISTORY", f"{MONGO_COLL}_history")
            hist = (micro
                .withColumn("created_day", F.to_date("created_at"))
                .withColumn("processed_day", F.to_date("processed_at"))
                .withColumn("_id", F.concat_ws("_", F.col("ids"),
                                               F.date_format(F.col("created_day"), "yyyyMMdd")))
                .select("_id", "ids", F.col("ids").alias("tweet_id"),
                        "text", "clean_text",
                        "sentiment", "sentiment_label", "p_neu", "p_pos",
                        "created_at", "created_day", "processed_day",
                        "category", "type", "origin", "lang", "source", "hasLink")
                .where(F.col("ids").isNotNull() & (F.length("ids") > 0)))
            (hist.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
                .option("spark.mongodb.write.database", MONGO_DB)
                .option("spark.mongodb.write.collection", mongo_coll_hist)
                .option("spark.mongodb.write.operationType", "replace")
                .mode("append")
                .save())
        except (Py4JJavaError, Exception) as e:
            msg = f"mongo_hist:{type(e).__name__}:{str(e)[:200]}"
            print("[foreachBatch] ERROR", msg, flush=True)
            err_msgs.append(msg)

        # ---------- METRICS + ERRORS ----------
        try:
            metrics_out = metrics_df if not err_msgs else metrics_df.withColumn("errors", F.lit(";".join(err_msgs)))
            (metrics_out.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
                .option("spark.mongodb.write.database", MONGO_DB)
                .option("spark.mongodb.write.collection", MONGO_COLL_METRICS)
                .mode("append")
                .save())
        except (Py4JJavaError, Exception):
            pass  # don't fail batch on metrics

    except Exception:
        print("[foreachBatch] FATAL:\n" + traceback.format_exc(), flush=True)
        raise
    finally:
        if micro is not None:
            micro.unpersist()


spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")

# ---- streaming query ----
main_query = (
    scored_df.writeStream
    .foreachBatch(write_to_sinks)
    .option("checkpointLocation", unified_ckpt)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print("[scorer] stream started:", main_query.id, flush=True)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    pass









































###############################################################################
# Real-time Twitter Sentiment Scorer — TRAINING-PARITY FE + INFERENCE LOGIC
# (drop-in for your spark_scorer)
###############################################################################

# ===== imports & env =====
import os, re, json, string, joblib, numpy as np, pandas as pd, logging

os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
from pyspark import StorageLevel
from py4j.protocol import Py4JJavaError
import traceback
from pyspark import SparkFiles
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel
from pyspark.ml.feature import MinMaxScalerModel, VectorAssembler, Tokenizer, StopWordsRemover, NGram
from pyspark.sql.functions import udf, pandas_udf
from pyspark.ml.functions import vector_to_array

# ===== logger =====
try:
    logger  # noqa
except NameError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("scorer")

# ===== paths / config =====
HERE      = os.path.dirname(__file__)
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "tweets")
OUT_TOPIC = os.getenv("OUT_TOPIC", "twitter_sentiment")
STARTING  = os.getenv("STARTING_OFFSETS", "earliest")
CKPT_BASE = os.getenv("CKPT_BASE", "/home/ubuntu/app/sentiment_realtime_project/checkpoints_v2")
ART_DIR   = os.getenv("BERTWEET_ART_DIR", os.path.join(BASE_DIR, "bertweet_artifacts"))

MONGO_URI_BASE = os.getenv("MONGO_URI_BASE", "").rstrip("/")
MONGO_DB       = os.getenv("MONGO_DB", "twitter_rt")
MONGO_COLL     = os.getenv("MONGO_COLL", "scored_tweets")

# shipped file names (provided via --files)
ONNX_PATH       = os.getenv("PN_ONNX_PATH", "pn_bertweet.onnx")
GATE_PKL        = os.getenv("GATE_PKL", "gate_xgb.pkl")
THRESHOLDS_JSON = os.getenv("THRESHOLDS_JSON", "thresholds_and_config.json")

HF_DIR    = os.getenv("HF_DIR", os.path.join(ART_DIR, "hf"))

# models (local dirs, not archives)
TFIDF_PATH  = os.getenv("TFIDF_MODEL_PATH",  "tfidf_model_shared")
SCALER_PATH = os.getenv("SCALER_MODEL_PATH", "minmax_scaler_model_shared")

# ===== Spark =====


spark = (
    SparkSession.builder
    .appName("TwitterSentimentScorer-HierONNX")
    .getOrCreate()
)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "64")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("WARN")

# ===== shipped-files resolver =====
def _resolve_local(name_or_path: str) -> str:
    base = os.path.basename(name_or_path)
    local = SparkFiles.get(base)
    return local if os.path.exists(local) else name_or_path

PN_PATH_RESOLVED   = _resolve_local(ONNX_PATH)
GATE_PATH_RESOLVED = _resolve_local(GATE_PKL)
THRESHOLDS_PATH    = _resolve_local(THRESHOLDS_JSON)

# ---- load gate & meta ----
gate = joblib.load(GATE_PATH_RESOLVED)
try:
    with open(THRESHOLDS_PATH) as f:
        meta = json.load(f)
except FileNotFoundError:
    meta = {}

# thresholds (from meta first, else env, else defaults)
gate_t = float(meta.get("gate_t", os.getenv("GATE_T", meta.get("t_neutral", 0.76))))
pn_t   = float(meta.get("pn_t",   os.getenv("PN_T",   0.47)))


GATE_NEU_CLASS = int(meta.get("gate_neutral_class",
                      os.getenv("GATE_NEU_CLASS", os.getenv("gate_neutral_class", "1"))))

BERT_BATCH = int(os.getenv("BERT_BATCH", "16"))
MAX_LEN    = int(os.getenv("MAX_LEN", str(meta.get("max_len", 224))))

logger.info(
    "Loaded gate; gate_t=%.3f pn_t=%.3f neutral_class=%s thresholds_json=%s",
    gate_t, pn_t, GATE_NEU_CLASS, THRESHOLDS_PATH
)

# 👉 ADD THIS HERE
try:
    logger.info("gate.classes_=%s  (neutral_class=%s)", getattr(gate, "classes_", None), GATE_NEU_CLASS)
except Exception:
    pass

# ===== load artifacts that need SparkContext =====
tfidf_model  = PipelineModel.load(TFIDF_PATH)
scaler_model = MinMaxScalerModel.load(SCALER_PATH)


# ===== sentence embeddings (CPU) =====
from sentence_transformers import SentenceTransformer
EMBED_MODEL_NAME = os.getenv("SENTENCE_MODEL_NAME", "all-MiniLM-L6-v2")
EMB_DIM = int(os.getenv("EMB_DIM", "384"))  # default/fallback
_embedder = None  # <-- add this

def _lazy_embedder():
    """Load sentence transformer once per worker (fail-soft)."""
    global _embedder, EMB_DIM
    if _embedder is None:
        try:
            _embedder = SentenceTransformer(EMBED_MODEL_NAME)
            try:
                EMB_DIM = int(_embedder.get_sentence_embedding_dimension())
            except Exception:
                pass
        except Exception:
            _embedder = False
    return _embedder

emb = _lazy_embedder()
logger.info("embedder: %s dim=%s", type(emb).__name__ if emb else emb, EMB_DIM)


# ===== PN ONNX (lazy per-executor) =====
import onnxruntime as ort
from transformers import AutoTokenizer

_onnx_sess = None
_onnx_tok  = None

def _lazy_onnx():
    global _onnx_sess, _onnx_tok
    if _onnx_sess is None:
        sess_opts = ort.SessionOptions()
        providers = ["CPUExecutionProvider"]
        _onnx_sess = ort.InferenceSession(
            PN_PATH_RESOLVED, sess_options=sess_opts, providers=providers
        )
        # prefer local HF bundle (offline)
        tok_dir = HF_DIR if os.path.isdir(HF_DIR) else "vinai/bertweet-base"
        _onnx_tok = AutoTokenizer.from_pretrained(
            tok_dir, local_files_only=True, use_fast=False, normalization=True
        )
    return _onnx_sess, _onnx_tok

# --- language detection helpers ---
ISO2_TO_3 = {
    "en":"eng","ur":"urd","ar":"ara","tr":"tur","es":"spa","fr":"fra","de":"deu","it":"ita",
    "pt":"por","hi":"hin","fa":"fas","zh":"zho","ja":"jpn","ko":"kor","ru":"rus","uk":"ukr",
    "pl":"pol","nl":"nld","sv":"swe","fi":"fin","id":"ind","ms":"msa","vi":"vie","th":"tha",
    "bn":"ben","ta":"tam","te":"tel","gu":"guj","mr":"mar","pa":"pan","he":"heb","el":"ell",
    "ro":"ron","cs":"ces","sk":"slk","sr":"srp","bg":"bul","hu":"hun"
}
def _to_iso3_py(code:str|None)->str:
    if not code: return "und"
    c = code.strip().lower()
    if len(c)==3:  # already iso-3?
        return c
    return ISO2_TO_3.get(c, "und")

try:
    import langid
    _have_langid = True
except Exception:
    _have_langid = False

@udf(T.StringType())
def detect_lang_udf(s):
    txt = (s or "").strip()
    if len(txt) < 6:
        # super short → guess english if mostly ascii letters/spaces, else und
        import re, string
        if txt and all((ch in string.printable) for ch in txt):
            return "eng"
        return "und"
    if _have_langid:
        try:
            code2 = langid.classify(txt)[0]  # e.g., 'en'
            return _to_iso3_py(code2)
        except Exception:
            pass
    # lightweight heuristic fallback
    try:
        ascii_ratio = sum(ch.isascii() for ch in txt)/max(len(txt),1)
        return "eng" if ascii_ratio > 0.95 else "und"
    except Exception:
        return "und"

@udf(T.StringType())
def to_iso3(code):
    return _to_iso3_py(code)

# ===== cleaning helpers (TRAINING-PARITY) =====
_word_re = re.compile(r"\s+")
def _clean_py(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower()
    s = re.sub(r"http\S+|www\S+", " ", s)   # urls
    s = re.sub(r"@\w+", " ", s)             # mentions
    s = re.sub(r"#", " ", s)                # drop '#', keep token
    s = re.sub(r"[^\x00-\x7f]", " ", s)     # non-ascii
    # *** parity line: compress elongations, don't delete them ***
    s = re.sub(r"(.)\1{2,}", r"\1\1", s)
    s = re.sub(f"[{re.escape(string.punctuation)}]", " ", s)  # punct
    s = _word_re.sub(" ", s).strip()
    return s


@udf(T.StringType())
def clean_text_udf(s): return _clean_py(s)

# ===== constants (same as training) =====
SENTIMENT_KW = F.array(*[F.lit(x) for x in
    ["love","hate","amazing","awful","great","bad","good","terrible","worst","excellent"]])
NEG_WORDS    = F.array(*[F.lit(x) for x in
    ["not","no","never","none","nobody","isn't","wasn't","aren't","don't","didn't","won't","can't"]])

# ===== VADER (broadcast) + training-style bucket =====
from nltk.sentiment.vader import SentimentIntensityAnalyzer
_vader = SentimentIntensityAnalyzer()
bc_vader = spark.sparkContext.broadcast(_vader)

@udf(T.DoubleType())
def vader_compound(txt):
    try:
        return float(bc_vader.value.polarity_scores(txt or "").get("compound", 0.0))
    except Exception:
        return 0.0

def add_vader(df):
    df = df.withColumn("vader_score", vader_compound("clean_text"))
    df = df.withColumn(
        "vader_bucket",
        F.when(F.col("vader_score") <= -0.4, F.lit(0))
         .when(F.col("vader_score") >= 0.4, F.lit(1))
         .otherwise(F.lit(2)).cast("int")
    )
    return df

# ===== light lexicons (training-style) + opinion_lexicon (fail-soft) =====
positive_lexicon = {"good","great","excellent","amazing","love","nice","happy","best"}
negative_lexicon = {"bad","terrible","awful","hate","worst","sad","horrible","angry"}

pos_bv = spark.sparkContext.broadcast(positive_lexicon)
neg_bv = spark.sparkContext.broadcast(negative_lexicon)

@udf(T.IntegerType())
def count_pos(text): return sum(1 for w in (text or "").split() if w in pos_bv.value)
@udf(T.IntegerType())
def count_neg(text): return sum(1 for w in (text or "").split() if w in neg_bv.value)

try:
    from nltk.corpus import opinion_lexicon
    ext_pos_bv = spark.sparkContext.broadcast(set(opinion_lexicon.positive()))
    ext_neg_bv = spark.sparkContext.broadcast(set(opinion_lexicon.negative()))
except Exception:
    ext_pos_bv = spark.sparkContext.broadcast(set())
    ext_neg_bv = spark.sparkContext.broadcast(set())

@udf(T.IntegerType())
def count_ext_pos(text): return sum(1 for w in (text or "").split() if w in ext_pos_bv.value)
@udf(T.IntegerType())
def count_ext_neg(text): return sum(1 for w in (text or "").split() if w in ext_neg_bv.value)

# ===== time features (training-style) =====
def add_time_cols(df):
    cands = []

    # Prefer already-parsed event_time if present
    if "event_time" in df.columns:
        cands.append(F.col("event_time").cast("timestamp"))

    # Twitter string date (rare in your stream)
    if "date" in df.columns:
        cands.append(F.to_timestamp(F.col("date"), "EEE MMM dd HH:mm:ss Z yyyy"))

    # created_at might be epoch (ms or sec) or string — handle both
    if "created_at" in df.columns:
        dt = df.schema["created_at"].dataType
        if isinstance(dt, T.LongType):
            # assume milliseconds; adjust if your producer sends seconds
            cands.append(F.to_timestamp(F.from_unixtime(F.col("created_at")/1000.0)))
        else:
            cands.append(F.to_timestamp(F.col("created_at")))

    # generic 'timestamp' column if exists
    if "timestamp" in df.columns:
        cands.append(F.col("timestamp").cast("timestamp"))

    # your 'ts' is epoch-ms (as per upstream usage)
    if "ts" in df.columns:
        cands.append(F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))

    # fold coalesces only over existing exprs
    ts = None
    for expr in cands:
        ts = expr if ts is None else F.coalesce(ts, expr)
    if ts is None:
        ts = F.current_timestamp()

    df = df.withColumn("date_ts", ts)
    df = (df
          .withColumn("hour", F.hour("date_ts"))
          .withColumn("day_of_week", F.dayofweek("date_ts"))
          .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0))
          )
    return df


from pyspark.sql.functions import udf
EMOJI_PY = re.compile(r'[\U0001F300-\U0001FAFF\U00002600-\U000026FF]')
EMOTI_PY = re.compile(r'(?:(?:[:;=8][\-^]?[)DpP/\\|oO\(\]])|<3|:\'\(|xD|XD)')

@udf(T.IntegerType())
def _count_emoji_like(text):
    if not text: return 0
    return len(EMOJI_PY.findall(text)) + len(EMOTI_PY.findall(text))
# ===== syntactic/semantic flags & counts (training-parity) =====
def add_flags_and_counts(df):
    df = (df
      .withColumn("text_lower", F.lower("text"))
      .withColumn("tokens", F.split("clean_text", r"\s+"))
      .withColumn("text_length", F.length("clean_text"))
      .withColumn("word_count", F.when(F.col("clean_text")=="" , 0)
                                  .otherwise(F.size(F.split("clean_text"," "))))
      .withColumn("char_density", F.when(F.col("word_count")==0, F.lit(0.0))
                                   .otherwise(F.col("text_length")/F.col("word_count")))
      .withColumn("char_density", F.when(F.col("char_density")>20.0, 20.0).otherwise(F.col("char_density")))
      .withColumn("has_mentions", F.when(F.col("text").rlike(r'(^|\s)@\w+'), 1).otherwise(0))
      .withColumn("has_hashtags", F.when(F.col("text").rlike(r'(^|\s)#\w+'), 1).otherwise(0))
      .withColumn("has_links",   ((F.instr("text_lower", "http") > 0) | (F.instr("text_lower", "www") > 0)).cast("int"))
      .withColumn("is_question", F.when(F.col("text").rlike(r'\?\s*$'), 1).otherwise(0))
      .withColumn("sentiment_keyword_count",
                  F.size(F.array_intersect(F.col("tokens"), SENTIMENT_KW)))
      .withColumn("negation_count",
                  F.size(F.array_intersect(F.col("tokens"), NEG_WORDS)))
      .withColumn("capital_word_count", F.size(F.expr(
          "filter(split(text,'\\s+'), x -> x = upper(x) AND length(x) >= 2)"
      )))
      .withColumn("emoji_count", _count_emoji_like(F.col("text")))
      .withColumn("positive_lexicon_count", count_pos("clean_text"))
      .withColumn("negative_lexicon_count", count_neg("clean_text"))
      .withColumn("extended_positive_lexicon_count", count_ext_pos("clean_text"))
      .withColumn("extended_negative_lexicon_count", count_ext_neg("clean_text"))
      .withColumn("punctuation_count",
                  F.length(F.regexp_replace(F.col("text"), f"[^{re.escape(string.punctuation)}]", "")))
    )
    # training-style: cap emojis to tame skew
    df = df.withColumn("emoji_count", F.least(F.col("emoji_count"), F.lit(5)))
    return df

# ===== log1p (same candidate list used in training) =====
LOG1P_COLS = [
    "text_length","word_count","char_density",
    "sentiment_keyword_count","negation_count",
    "emoji_count","capital_word_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
]
def apply_log1p(df):
    for c in LOG1P_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.log1p(F.col(c).cast("double")))
    return df

# ===== numeric features order — MUST MATCH TRAINING =====
NUMERIC_FEATURES = [
    "hour","day_of_week","is_weekend","text_length","word_count","char_density",
    "has_mentions","has_hashtags","has_links","is_question",
    "sentiment_keyword_count","negation_count",
    "emoji_count","capital_word_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
    "vader_score","vader_bucket"
]

def add_scaled_numeric(df):
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES,
        outputCol="numeric_raw",
        handleInvalid="keep"
    )
    df = assembler.transform(df)
    df = scaler_model.transform(df)
    df = df.withColumn("numeric_arr", vector_to_array("numeric_vec"))
    return df


# ===== TF-IDF pipeline parity (Tokenizer → StopWords → NGram(2) → merge) =====
merge_udf = F.udf(lambda uni, bi: (uni or []) + (bi or []),
                  T.ArrayType(T.StringType()))
_tok  = Tokenizer(inputCol="clean_text", outputCol="words")
_rem  = StopWordsRemover(inputCol="words", outputCol="words_clean")
_ng   = NGram(n=2, inputCol="words_clean", outputCol="bigrams")

def add_tfidf_cols(df):
    df = _tok.transform(df)
    df = _rem.transform(df)
    df = _ng.transform(df)
    df = df.withColumn("merged_ngrams", merge_udf(F.col("words_clean"), F.col("bigrams")))
    df = tfidf_model.transform(df)               # expects merged_ngrams
    df = df.withColumn("tfidf_arr", vector_to_array("tfidf_vec"))
    return df

predict_schema = T.StructType([
    T.StructField("pred_label", T.IntegerType(),  nullable=True),  # 0=neg, 1=pos (sirf routed rows)
    T.StructField("p_neu",      T.DoubleType(),   nullable=True),  # gate: P(neutral)
    T.StructField("p_pos",      T.DoubleType(),   nullable=True),  # PN:   P(positive) (routed)
    T.StructField("err",        T.StringType(),   nullable=True),
])

@pandas_udf(predict_schema)
def predict_udf(text: pd.Series,
                clean_texts: pd.Series,
                tfidf_arr: pd.Series,
                numeric_arr: pd.Series) -> pd.DataFrame:
    N = len(clean_texts)
    # prealloc
    yhat   = np.zeros(N, dtype=np.int32)
    p_neu  = np.zeros(N, dtype=np.float32)
    p_pos  = np.zeros(N, dtype=np.float32)
    errs   = np.array([""]*N, dtype=object)

    if N == 0:
        return pd.DataFrame({"pred_label": yhat, "p_neu": p_neu, "p_pos": p_pos, "err": errs})

    # ---------- TF-IDF ----------
    try:
        tf_list = tfidf_arr.to_list()
        tf_mat  = np.vstack(tf_list).astype(np.float32, copy=False) if len(tf_list) else np.zeros((N,0), np.float32)
    except Exception as e:
        tf_mat  = np.zeros((N,0), np.float32)
        errs    = np.where(errs=="", f"tfidf_fail:{type(e).__name__}", errs)

    # ---------- numeric ----------
    try:
        nv_list = numeric_arr.to_list()
        nv_mat  = np.vstack(nv_list).astype(np.float32, copy=False) if len(nv_list) else np.zeros((N,0), np.float32)
    except Exception as e:
        nv_mat  = np.zeros((N,0), np.float32)
        errs    = np.where(errs=="", f"num_fail:{type(e).__name__}", errs)

    # ---------- embeddings ----------
    txts = clean_texts.fillna("").tolist()
    try:
        emb = _lazy_embedder()
        if emb is False:
            raise RuntimeError("embedder_disabled")
        emb_mat = emb.encode(
            txts, batch_size=64, convert_to_numpy=True,
            show_progress_bar=False, normalize_embeddings=True
        ).astype(np.float32, copy=False)
    except Exception as e:
        emb_mat = np.zeros((N, EMB_DIM), dtype=np.float32)
        errs    = np.where(errs=="", f"embed_fail:{type(e).__name__}", errs)

    # ---------- full feature matrix ----------
    try:
        X = np.hstack([tf_mat, nv_mat, emb_mat]).astype(np.float32, copy=False)
    except Exception as e:
        # degrade gracefully
        try:
            X = np.hstack([nv_mat, emb_mat]).astype(np.float32, copy=False)
        except Exception:
            X = emb_mat

    # ---------- GATE: P(neutral) ----------
    need_idx = np.array([], dtype=np.int64)
    try:
        proba   = gate.predict_proba(X)  # (N, C)
        classes = list(getattr(gate, "classes_", []))  # e.g., [0,2] or [0,1]
        if classes:
            try:
                idx_neu = classes.index(GATE_NEU_CLASS)   # aapne meta/env me 2 set kiya
            except ValueError:
                idx_neu = 1 if len(classes) > 1 else 0
        else:
            idx_neu = 1 if proba.shape[1] > 1 else 0

        p_neu    = proba[:, idx_neu].astype(np.float32, copy=False)
        need_idx = np.where(p_neu < float(gate_t))[0]      # sirf non-neutral route
    except Exception as e:
        # fail-soft: sab neutral maan lo (taake sab negative na bane)
        p_neu[:] = 1.0
        need_idx = np.array([], dtype=np.int64)
        errs     = np.where(errs=="", f"gate_fail:{type(e).__name__}", errs)

    # ---------- PN (ONNX): sirf routed rows ----------
    if need_idx.size > 0:
        try:
            sess, tok = _lazy_onnx()
            pos_t = float(pn_t)
            for start in range(0, need_idx.size, BERT_BATCH):
                sl = need_idx[start:start+BERT_BATCH]
                batch_txts = [txts[i] for i in sl]
                enc = tok(batch_txts, return_tensors="np", truncation=True, max_length=MAX_LEN, padding=True)
                logits = sess.run(None, {
                    "input_ids": enc["input_ids"],
                    "attention_mask": enc["attention_mask"]
                })[0]  # (B, 2)
                logits = logits - logits.max(axis=1, keepdims=True)
                exps   = np.exp(logits, dtype=np.float32)
                probs  = exps / exps.sum(axis=1, keepdims=True)
                p_batch = probs[:, 1].astype(np.float32, copy=False)  # index-1 = POSITIVE
                p_pos[sl] = p_batch
                yhat[sl]  = (p_batch >= pos_t).astype(np.int32)       # 1=pos, 0=neg
        except Exception as e:
            errs = np.where(errs=="", f"onnx_fail:{type(e).__name__}", errs)
            # routed rows ko default neg pe chhod do (yhat already 0)

    return pd.DataFrame({
        "pred_label": yhat.astype(np.int32),
        "p_neu": p_neu.astype(np.float64),
        "p_pos": p_pos.astype(np.float64),
        "err":   errs
    })

# ==================== PUBLIC ENTRY: build features then score ====================
def build_features_then_score(src_df):
    """
    src_df must contain at least: 'text' (string) and ideally a timestamp column:
      one of ['date' (Twitter format), 'created_at', 'timestamp', 'ts'].
    Returns src_df + ['clean_text','numeric_vec','tfidf_vec','pred_label','p_neu','p_pos','err'].
    """
    df = src_df.withColumn("text", F.col("text").cast("string"))
    df = df.withColumn("clean_text", clean_text_udf("text"))

    # ensure 'lang' column exists (just in case)
    if "lang" not in df.columns:
        df = df.withColumn("lang", F.lit(None).cast(T.StringType()))

    # ---- feature engineering (training parity) ----
    df = add_time_cols(df)
    df = add_flags_and_counts(df)
    df = add_vader(df)
    df = add_scaled_numeric(df)   # adds numeric_vec + numeric_arr
    df = add_tfidf_cols(df)       # adds tfidf_vec  + tfidf_arr

 # ---- predictions (gate + PN via predict_udf) ----
    df = (
        df.withColumn(
            "pred_struct",
            predict_udf(
                F.col("text"),
                F.col("clean_text"),
                F.col("tfidf_arr"),
                F.col("numeric_arr"),
            )
        )
        .withColumns({
            "pred_label": F.col("pred_struct.pred_label"),
            "p_neu":      F.col("pred_struct.p_neu"),
            "p_pos":      F.col("pred_struct.p_pos"),
            "err":        F.col("pred_struct.err"),
        })
        .drop("pred_struct")
    )

    # ---- language fill (fallback only when null/empty/und & text long enough) ----
    df = df.withColumn("lang_pred", detect_lang_udf(F.col("clean_text")))
    df = df.withColumn(
        "lang",
        F.when(
            (F.col("lang").isNull() | (F.col("lang") == "") | (F.lower(F.col("lang")) == "und")) &
            (F.length(F.col("clean_text")) >= F.lit(5)),  # skip very short texts
            F.col("lang_pred")
        ).otherwise(F.col("lang"))
    )
    # map 2-letter -> 3-letter (en->eng, ur->urd, etc.)
    df = df.withColumn("lang", to_iso3(F.lower(F.col("lang"))))
    df = df.drop("lang_pred")

    return df


# --------------------------- Read from Kafka ---------------------------------
tweet_schema = T.StructType([
    T.StructField("ids", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("ts", T.LongType()),
    T.StructField("created_at", T.LongType()),
    T.StructField("category", T.StringType()),
    T.StructField("hasLink", T.BooleanType()),
    T.StructField("origin", T.StringType()),
    T.StructField("lang", T.StringType()),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", IN_TOPIC)
    .option("startingOffsets", STARTING)
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS value")
    .select(F.from_json("value", tweet_schema, {"mode": "PERMISSIVE"}).alias("data"))
    .where(F.col("data").isNotNull())
    .where(F.col("data.ids").isNotNull() & F.col("data.text").isNotNull())
    .where(F.length(F.col("data.text")) >= 3)
    .select("data.*")
)

if os.getenv("SCORER_DEBUG_CONSOLE", "false").lower() == "true":
    (parsed_df
        .select("ids", "text", "ts", "created_at", "category", "hasLink", "origin", "lang")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "5")
        .trigger(processingTime="10 seconds")
        .start())
    print("[debug] console sink for parsed_df is ON", flush=True)
dedup_in = (
    parsed_df
    .withColumn("event_time",
                F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))
    .withColumn("_id", F.col("ids").cast(T.StringType()))
    .withWatermark("event_time", "1 day")
    .dropDuplicates(["ids"])
)

# ==================== how to use in your stream ====================
base_for_pred = (
    dedup_in
    .select("ids","text","ts","event_time","category","hasLink","origin","lang")
    .withColumn("created_at_ts", F.col("event_time").cast(T.TimestampType()))
    .transform(build_features_then_score)     # <-- ye FE + arrays add karega
    .withColumn("tf_size", F.size("tfidf_arr"))
    .withColumn("nv_size", F.size("numeric_arr"))
)

# ===== prediction + final shaping ============================================
out = (
    base_for_pred
    .select(
        "ids","text","clean_text","ts","event_time","category","hasLink","origin","lang",
        "pred_label","p_neu","p_pos","err",
        F.coalesce(
            F.col("created_at_ts"),
            F.col("event_time").cast(T.TimestampType()),
            F.current_timestamp()
        ).alias("created_at"),
        F.current_timestamp().alias("processed_at"),
        "tf_size","nv_size"
    )
)


# ---- final 3-class label + consistent column names ----
scored_df = (
    out
    .withColumn("tweet_id", F.col("ids"))                                 # <- consistent id for sinks
    .withColumn("need_pn", (F.col("p_neu") < F.lit(float(gate_t))))
    .withColumn(                                                         # <- 3-class hard label
        "final_label",
        F.when(F.col("p_neu") >= F.lit(float(gate_t)), F.lit(2))         # 2 = neutral
         .otherwise(F.col("pred_label"))                                 # 0=neg, 1=pos
    )
    .withColumn("sentiment_label", F.col("final_label").cast("int"))
    .withColumn(                                                         # <- human-readable
        "sentiment",
        F.when(F.col("sentiment_label") == 2, F.lit("neutral"))
         .when(F.col("sentiment_label") == 1, F.lit("positive"))
         .otherwise(F.lit("negative"))
    )
    .withColumn("sentiment_text", F.col("sentiment"))                    # <- keep if downstream expects this
    .withColumn("need_pn_dbg", F.col("need_pn").cast("int"))
    .withColumn("gate_t", F.lit(float(gate_t)))
    .withColumn("pn_t",   F.lit(float(pn_t)))
    .withColumn("origin", F.coalesce(F.col("origin"), F.lit("live")))
    .withColumn("source", F.lit("scorer"))
    .withColumn("type",   F.lit("tweet"))
#    .withColumn("lang",   F.coalesce(F.col("lang"), F.lit("und")))
)

print("\n[scorer] ==== SCHEMAS & COLS ====", flush=True)
print("[scorer] scored_df schema:", flush=True); scored_df.printSchema()
print("[scorer] ========================\n", flush=True)
from pyspark.sql.utils import StreamingQueryException

# ======================= foreachBatch sinks ==================================
unified_ckpt = f"{CKPT_BASE}/unified_v1"

MONGO_COLL_METRICS = os.environ.get("MONGO_COLL_METRICS", f"{MONGO_COLL}_metrics")

def write_to_sinks(batch_df, epoch_id: int):
    micro = None
    err_msgs = []
    try:
        micro = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        rows = micro.count()
        print(f"[foreachBatch] epoch={epoch_id} rows={rows}", flush=True)

        # debug stats
        try:
            # --- label_dist WITHOUT toPandas ---
            dist_rows = (
                micro.groupBy("sentiment_label")
                     .count()
                     .orderBy("sentiment_label")
                     .collect()
            )
            print(
                "[dbg] label_dist:",
                [(r["sentiment_label"], r["count"]) for r in dist_rows],
                flush=True
            )

            routed = micro.agg(F.avg(F.col("need_pn").cast("double")).alias("routed_frac")).collect()[0]["routed_frac"]
            print(f"[dbg] routed_frac_to_PN={routed:.3f}, gate_t={gate_t}, pn_t={pn_t}", flush=True)

            pn_stats = micro.agg(
                F.expr("percentile_approx(p_pos, array(0.1,0.5,0.9))").alias("p_pos_pctls"),
                F.expr("percentile_approx(p_neu, array(0.1,0.5,0.9))").alias("p_neu_pctls")
            ).collect()[0]
            print("[dbg] p_pos pctls:", pn_stats["p_pos_pctls"], " p_neu pctls:", pn_stats["p_neu_pctls"], flush=True)
        except Exception as e:
            print("[dbg] metrics fail:", e, flush=True)

        if rows == 0:
            return

        # ---- MINI DEBUG SAMPLE (safe: limit + collect) ----
        try:
            # prefer event_time, else processed_at
            order_col = F.coalesce(F.col("event_time"), F.col("processed_at"))
            sample_rows = (
                micro
                .select(
                    "ids","lang","category","hasLink","clean_text",
                    "need_pn","final_label","sentiment","err","event_time","processed_at"
                )
                .orderBy(order_col.desc())
                .limit(10)
                .collect()
            )
            print(
                "[dbg] sample:",
                [
                    (
                        r.get("ids"),
                        r.get("lang"),
                        r.get("category"),
                        r.get("hasLink"),
                        (r.get("clean_text") or "")[:60],
                        r.get("need_pn"),
                        r.get("sentiment")
                    )
                    for r in sample_rows
                ],
                flush=True
            )
        except Exception as e:
            print("[dbg] sample ERROR:", repr(e), flush=True)
        # ---------------------------------------------------

        # metrics row
        has_sizes  = ("tf_size" in micro.columns) and ("nv_size" in micro.columns)
        gate_t_lit = F.lit(float(gate_t))
        aggs = [F.count(F.lit(1)).alias("rows")]
        if has_sizes:
            bad_cond = (F.col("tf_size") != F.lit(3000)) | (F.col("nv_size") != F.lit(21))
            aggs += [
                F.sum(F.when(bad_cond, 1).otherwise(0)).alias("bad_size_rows"),
                F.min("tf_size").alias("tf_min"), F.max("tf_size").alias("tf_max"),
                F.min("nv_size").alias("nv_min"), F.max("nv_size").alias("nv_max"),
            ]
        if "p_neu" in micro.columns:
            aggs += [
                F.avg(F.when(F.col("p_neu") >= gate_t_lit, 1.0).otherwise(0.0)).alias("frac_neutral_gate"),
                F.expr("percentile_approx(p_neu, array(0.01,0.1,0.5,0.9,0.99))").alias("p_neu_pctls")
            ]
        metrics_df = (micro.agg(*aggs)
                      .withColumn("batch_id", F.lit(int(epoch_id)))
                      .withColumn("ts", F.current_timestamp()))

        # ---- Kafka OUT ----
        try:
            kafka_df = micro.selectExpr(
                "CAST(ids AS STRING) AS key",
                "to_json(named_struct("
                " 'tweet_id', ids, "
                " 'text', text, "
                " 'clean_text', clean_text, "
                " 'sentiment', sentiment, "
                " 'sentiment_label', sentiment_label, "
                " 'p_neu', p_neu, "
                " 'p_pos', p_pos, "
                " 'created_at', created_at, "
                " 'processed_at', processed_at, "
                " 'type', type, "
                " 'category', category, "
                " 'hasLink', hasLink, "
                " 'origin', origin, "
                " 'lang', lang, "
                " 'source', source "
                ")) AS value"
            )
            (kafka_df.write
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("topic", OUT_TOPIC)
             .save())
        except (Py4JJavaError, Exception) as e:
            msg = f"kafka_write:{type(e).__name__}:{str(e)[:200]}"
            print("[foreachBatch] ERROR", msg, flush=True)
            err_msgs.append(msg)

        # ---- Mongo sinks (attempt regardless of Kafka result) ----
        try:
            latest = (micro
                      .withColumn("created_day", F.to_date("created_at"))
                      .withColumn("processed_day", F.to_date("processed_at"))
                      .withColumn("_id", F.col("ids"))
                      .select("_id", "ids", F.col("ids").alias("tweet_id"),
                              "text", "clean_text",
                              "sentiment", "sentiment_label", "p_neu", "p_pos",
                              "created_at", "created_day", "processed_day",
                              "category", "type", "origin", "lang", "source", "hasLink")
                      .where(F.col("_id").isNotNull() & (F.length("_id") > 0)))
            (latest.write
             .format("mongodb")
             .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
             .option("spark.mongodb.write.database", MONGO_DB)
             .option("spark.mongodb.write.collection", MONGO_COLL)
             .option("spark.mongodb.write.operationType", "replace")
             .mode("append")
             .save())
        except (Py4JJavaError, Exception) as e:
            msg = f"mongo_latest:{type(e).__name__}:{str(e)[:200]}"
            print("[foreachBatch] ERROR", msg, flush=True)
            err_msgs.append(msg)

        try:
            mongo_coll_hist = os.environ.get("MONGO_COLL_HISTORY", f"{MONGO_COLL}_history")
            hist = (micro
                    .withColumn("created_day", F.to_date("created_at"))
                    .withColumn("processed_day", F.to_date("processed_at"))
                    .withColumn("_id", F.concat_ws("_", F.col("ids"),
                                                   F.date_format(F.col("created_day"), "yyyyMMdd")))
                    .select("_id", "ids", F.col("ids").alias("tweet_id"),
                            "text", "clean_text",
                            "sentiment", "sentiment_label", "p_neu", "p_pos",
                            "created_at", "created_day", "processed_day",
                            "category", "type", "origin", "lang", "source", "hasLink")
                    .where(F.col("ids").isNotNull() & (F.length("ids") > 0)))
            (hist.write
             .format("mongodb")
             .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
             .option("spark.mongodb.write.database", MONGO_DB)
             .option("spark.mongodb.write.collection", mongo_coll_hist)
             .option("spark.mongodb.write.operationType", "replace")
             .mode("append")
             .save())
        except (Py4JJavaError, Exception) as e:
            msg = f"mongo_hist:{type(e).__name__}:{str(e)[:200]}"
            print("[foreachBatch] ERROR", msg, flush=True)
            err_msgs.append(msg)

        # ---- metrics out (best-effort) ----
        try:
            metrics_out = metrics_df if not err_msgs else metrics_df.withColumn("errors", F.lit(";".join(err_msgs)))
            (metrics_out.write
             .format("mongodb")
             .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
             .option("spark.mongodb.write.database", MONGO_DB)
             .option("spark.mongodb.write.collection", MONGO_COLL_METRICS)
             .mode("append")
             .save())
        except (Py4JJavaError, Exception):
            pass

    except Exception:
        print("[foreachBatch] FATAL:\n" + traceback.format_exc(), flush=True)
        raise
    finally:
        if micro is not None:
            micro.unpersist()



spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")

main_query = (
    scored_df.writeStream
    .foreachBatch(write_to_sinks)
    .option("checkpointLocation", unified_ckpt)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print("[scorer] stream started:", main_query.id, flush=True)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    pass










def write_to_sinks(batch_df, epoch_id: int):
    micro = None
    err_msgs = []
    try:
        micro = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        rows = micro.count()
        print(f"[foreachBatch] epoch={epoch_id} rows={rows}", flush=True)
    # debug stats
    try:
        # --- label_dist WITHOUT toPandas ---
        dist_rows = (
            micro.groupBy("sentiment_label")
            .count()
            .orderBy("sentiment_label")
            .collect()
        )
        print("[dbg] label_dist:", [(r["sentiment_label"], r["count"]) for r in dist_rows], flush=True)

        routed = micro.agg(F.avg(F.col("need_pn").cast("double")).alias("r")).collect()[0]["r"] or 0.0
        frac_neutral_gate = micro.agg(
            F.avg(F.when(F.col("p_neu") >= F.lit(float(gate_t)), 1.0).otherwise(0.0)).alias("f")
        ).collect()[0]["f"] or 0.0
        print(f"[dbg] routed_frac_to_PN={routed:.3f}  "
              f"frac_neutral_gate={frac_neutral_gate:.3f}  "
              f"gate_t={gate_t:.9f}  pn_t={pn_t}", flush=True)

        pn_only = micro.filter(F.col("need_pn"))
        if pn_only.take(1):  # faster than limit().count()
            pn_stats = pn_only.agg(
                F.expr("percentile_approx(p_pos, array(0.1,0.5,0.9))").alias("p_pos_pctls"),
                F.expr("percentile_approx(p_neu, array(0.1,0.5,0.9))").alias("p_neu_pctls")
            ).collect()[0]
            print("[dbg] PN-only pctls: p_pos", pn_stats["p_pos_pctls"],
                  " p_neu", pn_stats["p_neu_pctls"], flush=True)
        else:
            print("[dbg] PN-only pctls: (no PN rows this batch)", flush=True)

        pairs = (micro.groupBy("sentiment_label", "sentiment")
                 .count()
                 .orderBy("sentiment_label", "sentiment")
                 .collect())
        print("[dbg] mapping check:",
              [(int(r['sentiment_label']), r['sentiment'], int(r['count'])) for r in pairs],
              flush=True)

    except Exception as e:
        print("[dbg] metrics fail:", e, flush=True)

    if rows == 0:
        return





























































































































###############################################################################
# Real-time Twitter Sentiment Scorer — TRAINING-PARITY FE + INFERENCE LOGIC
# (drop-in for your spark_scorer)
###############################################################################

# ===== imports & env =====
import os, re, json, string, joblib, numpy as np, pandas as pd, logging

os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
from pyspark import StorageLevel
from py4j.protocol import Py4JJavaError
import traceback
from pyspark import SparkFiles
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel
from pyspark.ml.feature import MinMaxScalerModel, VectorAssembler, Tokenizer, StopWordsRemover, NGram
from pyspark.sql.functions import udf, pandas_udf
from pyspark.ml.functions import vector_to_array

# ===== logger =====
try:
    logger  # noqa
except NameError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("scorer")

# ===== paths / config =====
HERE      = os.path.dirname(__file__)
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "tweets")
OUT_TOPIC = os.getenv("OUT_TOPIC", "twitter_sentiment")
STARTING  = os.getenv("STARTING_OFFSETS", "earliest")
CKPT_BASE = os.getenv("CKPT_BASE", "/home/ubuntu/app/sentiment_realtime_project/checkpoints_v2")
ART_DIR   = os.getenv("BERTWEET_ART_DIR", os.path.join(BASE_DIR, "bertweet_artifacts"))


MONGO_URI_BASE = os.getenv("MONGO_URI_BASE", "").rstrip("/")
MONGO_DB       = os.getenv("MONGO_DB", "twitter_rt")
MONGO_COLL     = os.getenv("MONGO_COLL", "scored_tweets")

# shipped file names (provided via --files)
ONNX_PATH       = os.getenv("PN_ONNX_PATH", "pn_bertweet.onnx")
GATE_PKL        = os.getenv("GATE_PKL", "gate_xgb.pkl")
THRESHOLDS_JSON = os.getenv("THRESHOLDS_JSON", "thresholds_and_config.json")

HF_DIR    = os.getenv("HF_DIR", os.path.join(ART_DIR, "hf"))

# models (local dirs, not archives)
TFIDF_PATH  = os.getenv("TFIDF_MODEL_PATH",  "tfidf_model_shared")
SCALER_PATH = os.getenv("SCALER_MODEL_PATH", "minmax_scaler_model_shared")

# ===== Spark =====
spark = (
    SparkSession.builder
    .appName("TwitterSentimentScorer-HierONNX")
    .getOrCreate()
)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "64")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("WARN")

# ===== shipped-files resolver =====
def _resolve_local(name_or_path: str) -> str:
    base = os.path.basename(name_or_path)
    local = SparkFiles.get(base)
    return local if os.path.exists(local) else name_or_path

PN_PATH_RESOLVED   = _resolve_local(ONNX_PATH)
GATE_PATH_RESOLVED = _resolve_local(GATE_PKL)
THRESHOLDS_PATH    = _resolve_local(THRESHOLDS_JSON)

# ---- load gate & meta ----
gate = joblib.load(GATE_PATH_RESOLVED)
try:
    with open(THRESHOLDS_PATH) as f:
        meta = json.load(f)
except FileNotFoundError:
    meta = {}

# thresholds (from meta first, else env, else defaults)
gate_t = float(meta.get("gate_t", os.getenv("GATE_T", meta.get("t_neutral", 0.90))))
pn_t   = float(meta.get("pn_t",   os.getenv("PN_T",   0.40)))


GATE_NEU_CLASS = int(meta.get("gate_neutral_class",
                      os.getenv("GATE_NEU_CLASS", os.getenv("gate_neutral_class", "1"))))

BERT_BATCH = int(os.getenv("BERT_BATCH", "16"))
MAX_LEN    = int(os.getenv("MAX_LEN", str(meta.get("max_len", 224))))

logger.info(
    "Loaded gate; gate_t=%.3f pn_t=%.3f neutral_class=%s thresholds_json=%s",
    gate_t, pn_t, GATE_NEU_CLASS, THRESHOLDS_PATH
)

# 👉 ADD THIS HERE
try:
    logger.info("gate.classes_=%s  (neutral_class=%s)", getattr(gate, "classes_", None), GATE_NEU_CLASS)
except Exception:
    pass
# ===== load artifacts that need SparkContext =====
tfidf_model  = PipelineModel.load(TFIDF_PATH)
scaler_model = MinMaxScalerModel.load(SCALER_PATH)

def apply_runtime_filters(df, cfg):
    min_len    = int(cfg.get("min_text_len", 0))
    drop_links = bool(cfg.get("drop_links", False))
    lang_allow = cfg.get("lang_allow", [])

    if min_len > 0:
        df = df.filter(F.length(F.trim(F.col("clean_text"))) >= F.lit(min_len))
    if drop_links:
        df = df.filter(~F.col("hasLink"))
    if lang_allow:
        df = df.filter(F.col("lang").isin(lang_allow))
    return df

# ===== sentence embeddings (CPU) =====
from sentence_transformers import SentenceTransformer
EMBED_MODEL_NAME = os.getenv("SENTENCE_MODEL_NAME", "all-MiniLM-L6-v2")
EMB_DIM = int(os.getenv("EMB_DIM", "384"))  # default/fallback
_embedder = None  # <-- add this

def _lazy_embedder():
    """Load sentence transformer once per worker (fail-soft)."""
    global _embedder, EMB_DIM
    if _embedder is None:
        try:
            _embedder = SentenceTransformer(EMBED_MODEL_NAME)
            try:
                EMB_DIM = int(_embedder.get_sentence_embedding_dimension())
            except Exception:
                pass
        except Exception:
            _embedder = False
    return _embedder

emb = _lazy_embedder()
logger.info("embedder: %s dim=%s", type(emb).__name__ if emb else emb, EMB_DIM)

# ===== PN ONNX (lazy per-executor) =====
import onnxruntime as ort
from transformers import AutoTokenizer

_onnx_sess = None
_onnx_tok  = None

def _lazy_onnx():
    global _onnx_sess, _onnx_tok
    if _onnx_sess is None:
        sess_opts = ort.SessionOptions()
        providers = ["CPUExecutionProvider"]
        _onnx_sess = ort.InferenceSession(
            PN_PATH_RESOLVED, sess_options=sess_opts, providers=providers
        )
        # prefer local HF bundle (offline)
        tok_dir = HF_DIR if os.path.isdir(HF_DIR) else "vinai/bertweet-base"
        _onnx_tok = AutoTokenizer.from_pretrained(
            tok_dir, local_files_only=True, use_fast=False, normalization=True
        )
    return _onnx_sess, _onnx_tok

# --- language detection helpers ---
ISO2_TO_3 = {
    "en":"eng","ur":"urd","ar":"ara","tr":"tur","es":"spa","fr":"fra","de":"deu","it":"ita",
    "pt":"por","hi":"hin","fa":"fas","zh":"zho","ja":"jpn","ko":"kor","ru":"rus","uk":"ukr",
    "pl":"pol","nl":"nld","sv":"swe","fi":"fin","id":"ind","ms":"msa","vi":"vie","th":"tha",
    "bn":"ben","ta":"tam","te":"tel","gu":"guj","mr":"mar","pa":"pan","he":"heb","el":"ell",
    "ro":"ron","cs":"ces","sk":"slk","sr":"srp","bg":"bul","hu":"hun"
}

def _to_iso3_py(code:str|None)->str:
    if not code: return "und"
    c = code.strip().lower()
    if len(c)==3:  # already iso-3?
        return c
    return ISO2_TO_3.get(c, "und")

try:
    import langid
    _have_langid = True
except Exception:
    _have_langid = False

@udf(T.StringType())
def detect_lang_udf(s):
    txt = (s or "").strip()
    if len(txt) < 6:
        # super short → guess english if mostly ascii letters/spaces, else und
        import re, string
        if txt and all((ch in string.printable) for ch in txt):
            return "eng"
        return "und"
    if _have_langid:
        try:
            code2 = langid.classify(txt)[0]  # e.g., 'en'
            return _to_iso3_py(code2)
        except Exception:
            pass
    # lightweight heuristic fallback
    try:
        ascii_ratio = sum(ch.isascii() for ch in txt)/max(len(txt),1)
        return "eng" if ascii_ratio > 0.95 else "und"
    except Exception:
        return "und"

@udf(T.StringType())
def to_iso3(code):
    return _to_iso3_py(code)

# ===== cleaning helpers (TRAINING-PARITY) =====
_word_re = re.compile(r"\s+")
def _clean_py(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower()
    s = re.sub(r"http\S+|www\S+", " ", s)   # urls
    s = re.sub(r"@\w+", " ", s)             # mentions
    s = re.sub(r"#", " ", s)                # drop '#', keep token
    s = re.sub(r"[^\x00-\x7f]", " ", s)     # non-ascii
    # *** parity line: compress elongations, don't delete them ***
    s = re.sub(r"(.)\1{2,}", r"\1\1", s)
    s = re.sub(f"[{re.escape(string.punctuation)}]", " ", s)  # punct
    s = _word_re.sub(" ", s).strip()
    return s
@udf(T.StringType())
def clean_text_udf(s): return _clean_py(s)

# ===== constants (same as training) =====
SENTIMENT_KW = F.array(*[F.lit(x) for x in
    ["love","hate","amazing","awful","great","bad","good","terrible","worst","excellent"]])
NEG_WORDS    = F.array(*[F.lit(x) for x in
    ["not","no","never","none","nobody","isn't","wasn't","aren't","don't","didn't","won't","can't"]])

# ===== VADER (broadcast) + training-style bucket =====
from nltk.sentiment.vader import SentimentIntensityAnalyzer
_vader = SentimentIntensityAnalyzer()
bc_vader = spark.sparkContext.broadcast(_vader)

@udf(T.DoubleType())
def vader_compound(txt):
    try:
        return float(bc_vader.value.polarity_scores(txt or "").get("compound", 0.0))
    except Exception:
        return 0.0

def add_vader(df):
    df = df.withColumn("vader_score", vader_compound("clean_text"))
    df = df.withColumn(
        "vader_bucket",
        F.when(F.col("vader_score") <= -0.4, F.lit(0))
         .when(F.col("vader_score") >= 0.4, F.lit(1))
         .otherwise(F.lit(2)).cast("int")
    )
    return df

# ===== light lexicons (training-style) + opinion_lexicon (fail-soft) =====
positive_lexicon = {"good","great","excellent","amazing","love","nice","happy","best"}
negative_lexicon = {"bad","terrible","awful","hate","worst","sad","horrible","angry"}

pos_bv = spark.sparkContext.broadcast(positive_lexicon)
neg_bv = spark.sparkContext.broadcast(negative_lexicon)

@udf(T.IntegerType())
def count_pos(text): return sum(1 for w in (text or "").split() if w in pos_bv.value)
@udf(T.IntegerType())
def count_neg(text): return sum(1 for w in (text or "").split() if w in neg_bv.value)

try:
    from nltk.corpus import opinion_lexicon
    ext_pos_bv = spark.sparkContext.broadcast(set(opinion_lexicon.positive()))
    ext_neg_bv = spark.sparkContext.broadcast(set(opinion_lexicon.negative()))
except Exception:
    ext_pos_bv = spark.sparkContext.broadcast(set())
    ext_neg_bv = spark.sparkContext.broadcast(set())

@udf(T.IntegerType())
def count_ext_pos(text): return sum(1 for w in (text or "").split() if w in ext_pos_bv.value)
@udf(T.IntegerType())
def count_ext_neg(text): return sum(1 for w in (text or "").split() if w in ext_neg_bv.value)

# ===== time features (training-style) =====
def add_time_cols(df):
    cands = []

    # Prefer already-parsed event_time if present
    if "event_time" in df.columns:
        cands.append(F.col("event_time").cast("timestamp"))

    # Twitter string date (rare in your stream)
    if "date" in df.columns:
        cands.append(F.to_timestamp(F.col("date"), "EEE MMM dd HH:mm:ss Z yyyy"))

    # created_at might be epoch (ms or sec) or string — handle both
    if "created_at" in df.columns:
        dt = df.schema["created_at"].dataType
        if isinstance(dt, T.LongType):
            # assume milliseconds; adjust if your producer sends seconds
            cands.append(F.to_timestamp(F.from_unixtime(F.col("created_at")/1000.0)))
        else:
            cands.append(F.to_timestamp(F.col("created_at")))

    # generic 'timestamp' column if exists
    if "timestamp" in df.columns:
        cands.append(F.col("timestamp").cast("timestamp"))

    # your 'ts' is epoch-ms (as per upstream usage)
    if "ts" in df.columns:
        cands.append(F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))
        # fold coalesces only over existing exprs
    ts = None
    for expr in cands:
        ts = expr if ts is None else F.coalesce(ts, expr)
    if ts is None:
        ts = F.current_timestamp()

    df = df.withColumn("date_ts", ts)
    df = (df
          .withColumn("hour", F.hour("date_ts"))
          .withColumn("day_of_week", F.dayofweek("date_ts"))
          .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0))
          )
    return df

from pyspark.sql.functions import udf
EMOJI_PY = re.compile(r'[\U0001F300-\U0001FAFF\U00002600-\U000026FF]')
EMOTI_PY = re.compile(r'(?:(?:[:;=8][\-^]?[)DpP/\\|oO\(\]])|<3|:\'\(|xD|XD)')

@udf(T.IntegerType())
def _count_emoji_like(text):
    if not text: return 0
    return len(EMOJI_PY.findall(text)) + len(EMOTI_PY.findall(text))
# ===== syntactic/semantic flags & counts (training-parity) =====
def add_flags_and_counts(df):
    df = (df
      .withColumn("text_lower", F.lower("text"))
      .withColumn("tokens", F.split("clean_text", r"\s+"))
      .withColumn("text_length", F.length("clean_text"))
      .withColumn("word_count", F.when(F.col("clean_text")=="" , 0)
                                  .otherwise(F.size(F.split("clean_text"," "))))
      .withColumn("char_density", F.when(F.col("word_count")==0, F.lit(0.0))
                                   .otherwise(F.col("text_length")/F.col("word_count")))
      .withColumn("char_density", F.when(F.col("char_density")>20.0, 20.0).otherwise(F.col("char_density")))
      .withColumn("has_mentions", F.when(F.col("text").rlike(r'(^|\s)@\w+'), 1).otherwise(0))
      .withColumn("has_hashtags", F.when(F.col("text").rlike(r'(^|\s)#\w+'), 1).otherwise(0))
      .withColumn("is_question", F.when(F.col("text").rlike(r'\?\s*$'), 1).otherwise(0))
      .withColumn("sentiment_keyword_count", F.size(F.array_intersect(F.col("tokens"), SENTIMENT_KW)))
      .withColumn("negation_count", F.size(F.array_intersect(F.col("tokens"), NEG_WORDS)))
      .withColumn("capital_word_count", F.size(F.expr(
          "filter(split(text,'\\s+'), x -> x = upper(x) AND length(x) >= 2)"
      )))
      .withColumn("emoji_count", _count_emoji_like(F.col("text")))
          .withColumn("positive_lexicon_count", count_pos("clean_text"))
          .withColumn("negative_lexicon_count", count_neg("clean_text"))
          .withColumn("extended_positive_lexicon_count", count_ext_pos("clean_text"))
          .withColumn("extended_negative_lexicon_count", count_ext_neg("clean_text"))
          .withColumn("punctuation_count",
                      F.length(F.regexp_replace(F.col("text"), f"[^{re.escape(string.punctuation)}]", "")))
          # single, canonical link flag (boolean)
          .withColumn("hasLink", F.lower(F.col("text")).rlike("http|www|t\\.co"))
          )
    # cap emojis like training
    df = df.withColumn("emoji_count", F.least(F.col("emoji_count"), F.lit(5)))
    return df
# ===== log1p (same candidate list used in training) =====
LOG1P_COLS = [
    "text_length","word_count","char_density",
    "sentiment_keyword_count","negation_count",
    "emoji_count","capital_word_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
]
def apply_log1p(df):
    for c in LOG1P_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.log1p(F.col(c).cast("double")))
    return df

# ===== numeric features order — MUST MATCH TRAINING =====
NUMERIC_FEATURES = [
    "hour","day_of_week","is_weekend","text_length","word_count","char_density",
    "has_mentions","has_hashtags","has_links","is_question",
    "sentiment_keyword_count","negation_count",
    "emoji_count","capital_word_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
    "vader_score","vader_bucket"
]

def add_scaled_numeric(df):
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES,
        outputCol="numeric_raw",
        handleInvalid="keep"
    )
    df = assembler.transform(df)
    df = scaler_model.transform(df)
    df = df.withColumn("numeric_arr", vector_to_array("numeric_vec"))
    return df


# ===== TF-IDF pipeline parity (Tokenizer → StopWords → NGram(2) → merge) =====
merge_udf = F.udf(lambda uni, bi: (uni or []) + (bi or []),
                  T.ArrayType(T.StringType()))
_tok  = Tokenizer(inputCol="clean_text", outputCol="words")
_rem  = StopWordsRemover(inputCol="words", outputCol="words_clean")
_ng   = NGram(n=2, inputCol="words_clean", outputCol="bigrams")

def add_tfidf_cols(df):
    df = _tok.transform(df)
    df = _rem.transform(df)
    df = _ng.transform(df)
    df = df.withColumn("merged_ngrams", merge_udf(F.col("words_clean"), F.col("bigrams")))
    df = tfidf_model.transform(df)               # expects merged_ngrams
    df = df.withColumn("tfidf_arr", vector_to_array("tfidf_vec"))
    return df

predict_schema = T.StructType([
    T.StructField("pred_label", T.IntegerType(),  nullable=True),  # 0=neg, 1=pos (sirf routed rows)
    T.StructField("p_neu",      T.DoubleType(),   nullable=True),  # gate: P(neutral)
    T.StructField("p_pos",      T.DoubleType(),   nullable=True),  # PN:   P(positive) (routed)
    T.StructField("err",        T.StringType(),   nullable=True),
])

@pandas_udf(predict_schema)
def predict_udf(text: pd.Series,
                clean_texts: pd.Series,
                tfidf_arr: pd.Series,
                numeric_arr: pd.Series) -> pd.DataFrame:
    N = len(clean_texts)

    # prealloc
    yhat  = np.zeros(N, dtype=np.int32)
    p_neu = np.zeros(N, dtype=np.float32)
    p_pos = np.zeros(N, dtype=np.float32)
    errs  = np.array([""] * N, dtype=object)

    if N == 0:
        return pd.DataFrame(
            {"pred_label": yhat, "p_neu": p_neu, "p_pos": p_pos, "err": errs}
        )
        # ---------- TF-IDF ----------
    try:
        tf_list = tfidf_arr.to_list()
        tf_mat = (
            np.vstack(tf_list).astype(np.float32, copy=False)
            if len(tf_list) else np.zeros((N, 0), np.float32)
        )
    except Exception as e:
        tf_mat = np.zeros((N, 0), np.float32)
        errs = np.where(errs == "", f"tfidf_fail:{type(e).__name__}", errs)

        # ---------- numeric ----------
    try:
        nv_list = numeric_arr.to_list()
        nv_mat = (
            np.vstack(nv_list).astype(np.float32, copy=False)
            if len(nv_list) else np.zeros((N, 0), np.float32)
        )
    except Exception as e:
        nv_mat = np.zeros((N, 0), np.float32)
        errs = np.where(errs == "", f"num_fail:{type(e).__name__}", errs)
        # ---------- embeddings ----------
    txts = clean_texts.fillna("").tolist()
    try:
        emb = _lazy_embedder()
        if emb is False:
            raise RuntimeError("embedder_disabled")
        emb_mat = emb.encode(
            txts,
            batch_size=64,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        ).astype(np.float32, copy=False)
    except Exception as e:
        emb_mat = np.zeros((N, EMB_DIM), dtype=np.float32)
        errs = np.where(errs == "", f"embed_fail:{type(e).__name__}", errs)
        # ---------- full feature matrix ----------
    try:
        X = np.hstack([tf_mat, nv_mat, emb_mat]).astype(np.float32, copy=False)
    except Exception:
        # degrade gracefully
        try:
            X = np.hstack([nv_mat, emb_mat]).astype(np.float32, copy=False)
        except Exception:
            X = emb_mat
        # ---------- GATE: P(neutral) ----------
    need_idx = np.array([], dtype=np.int64)
    try:
        proba = gate.predict_proba(X)  # (N, C)
        classes = np.array(getattr(gate, "classes_", []))

        # try training convention: class "1" == neutral
        try:
            idx_neu = int(np.where(classes == 1)[0][0])
        except Exception:
            # fallback: try env/global GATE_NEU_CLASS if present
            try:
                idx_neu = list(classes).index(GATE_NEU_CLASS)  # may raise
            except Exception:
                idx_neu = 1 if proba.shape[1] > 1 else 0
                if "logger" in globals():
                    try:
                        logger.warning(
                            "Falling back idx_neu=%d for classes=%s",
                            idx_neu,
                            classes.tolist(),
                        )
                    except Exception:
                        pass

        p_neu = proba[:, idx_neu].astype(np.float32, copy=False)
        # optional canary flip if distribution looks off (helps when column mapping inverted)
        med = float(np.nanmedian(p_neu)) if p_neu.size else 0.0
        if med < 0.05 and proba.shape[1] == 2:
            if "logger" in globals():
                try:
                    logger.warning(
                        "p_neu median very low (%.3f) — trying flipped column once.", med
                    )
                except Exception:
                    pass
            p_neu = proba[:, 1 - idx_neu].astype(np.float32, copy=False)

        need_idx = np.where(p_neu < float(gate_t))[0]  # sirf non-neutral route

except Exception as e:
# fail-soft: sab neutral maan lo (taake sab negative na bane)
p_neu[:] = 1.0
need_idx = np.array([], dtype=np.int64)
errs = np.where(errs == "", f"gate_fail:{type(e).__name__}", errs)
if need_idx.size > 0:
    try:
        sess, tok = _lazy_onnx()
        pos_t = float(pn_t)
        for start in range(0, need_idx.size, BERT_BATCH):
            sl = need_idx[start: start + BERT_BATCH]
            batch_txts = [txts[i] for i in sl]
            enc = tok(
                batch_txts,
                return_tensors="np",
                truncation=True,
                max_length=MAX_LEN,
                padding=True,
            )
            logits = sess.run(
                None,
                {
                    "input_ids": enc["input_ids"],
                    "attention_mask": enc["attention_mask"],
                },
            )[0]  # (B, 2)
            logits = logits - logits.max(axis=1, keepdims=True)
            exps = np.exp(logits, dtype=np.float32)
            probs = exps / exps.sum(axis=1, keepdims=True)
            p_batch = probs[:, 1].astype(np.float32, copy=False)  # index-1 = POS
            p_pos[sl] = p_batch
            yhat[sl] = (p_batch >= pos_t).astype(np.int32)  # 1=pos, 0=neg
    except Exception as e:
        errs = np.where(errs == "", f"onnx_fail:{type(e).__name__}", errs)
        # routed rows default neg (yhat already 0)

        return pd.DataFrame(
            {
                "pred_label": yhat.astype(np.int32),
                "p_neu": p_neu.astype(np.float64),
                "p_pos": p_pos.astype(np.float64),
                "err": errs,
            }
        )


# ==================== PUBLIC ENTRY: build features then score ====================
def build_features_then_score(src_df):
    """
    src_df must contain at least: 'text' (string) and ideally a timestamp column:
      one of ['date' (Twitter format), 'created_at', 'timestamp', 'ts'].
    Returns src_df + ['clean_text','numeric_vec','tfidf_vec','pred_label','p_neu','p_pos','err'].
    """
    df = src_df.withColumn("text", F.col("text").cast("string"))
    df = df.withColumn("clean_text", clean_text_udf("text"))

    # ensure 'lang' column exists (just in case)
    if "lang" not in df.columns:
        df = df.withColumn("lang", F.lit(None).cast(T.StringType()))

    # ---- feature engineering (training parity) ----
    df = add_time_cols(df)
    df = add_flags_and_counts(df)
    df = add_vader(df)
    df = add_scaled_numeric(df)   # adds numeric_vec + numeric_arr
    df = add_tfidf_cols(df)       # adds tfidf_vec  + tfidf_arr

 # ---- predictions (gate + PN via predict_udf) ----
    df = (
        df.withColumn(
            "pred_struct",
            predict_udf(
                F.col("text"),
                F.col("clean_text"),
                F.col("tfidf_arr"),
                F.col("numeric_arr"),
            )
        )
        .withColumns({
            "pred_label": F.col("pred_struct.pred_label"),
            "p_neu":      F.col("pred_struct.p_neu"),
            "p_pos":      F.col("pred_struct.p_pos"),
            "err":        F.col("pred_struct.err"),
        })
        .drop("pred_struct")
    )

    # ---- language fill (fallback only when null/empty/und & text long enough) ----
    df = df.withColumn("lang_pred", detect_lang_udf(F.col("clean_text")))
    df = df.withColumn(
        "lang",
        F.when(
            (F.col("lang").isNull() | (F.col("lang") == "") | (F.lower(F.col("lang")) == "und")) &
            (F.length(F.col("clean_text")) >= F.lit(5)),  # skip very short texts
            F.col("lang_pred")
        ).otherwise(F.col("lang"))
    )
# map 2-letter -> 3-letter (en->eng, ur->urd, etc.)
    df = df.withColumn("lang", to_iso3(F.lower(F.col("lang"))))
    df = df.drop("lang_pred")

    return df


# --------------------------- Read from Kafka ---------------------------------
tweet_schema = T.StructType([
    T.StructField("ids", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("ts", T.LongType()),
    T.StructField("created_at", T.LongType()),
    T.StructField("category", T.StringType()),
    T.StructField("hasLink", T.BooleanType()),
    T.StructField("origin", T.StringType()),
    T.StructField("lang", T.StringType()),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", IN_TOPIC)
    .option("startingOffsets", STARTING)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "2000")   # ← add this (tune 1000–5000)
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS value")
    .select(F.from_json("value", tweet_schema, {"mode": "PERMISSIVE"}).alias("data"))
    .where(F.col("data").isNotNull())
    .where(F.col("data.ids").isNotNull() & F.col("data.text").isNotNull())
    .where(F.length(F.col("data.text")) >= 3)
    .select("data.*")
)

if os.getenv("SCORER_DEBUG_CONSOLE", "false").lower() == "true":
    (parsed_df
        .select("ids", "text", "ts", "created_at", "category", "hasLink", "origin", "lang")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "5")
        .trigger(processingTime="10 seconds")
        .start())
    print("[debug] console sink for parsed_df is ON", flush=True)
dedup_in = (
    parsed_df
    .withColumn("event_time",
                F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))
    .withColumn("_id", F.col("ids").cast(T.StringType()))
    .withWatermark("event_time", "1 day")
    .dropDuplicates(["ids"])
)


# ==================== how to use in your stream ====================
base_for_pred = (
    dedup_in
    .select("ids","text","ts","event_time","category","hasLink","origin","lang")
    .withColumn("created_at_ts", F.col("event_time").cast(T.TimestampType()))
    .transform(build_features_then_score)     # <-- ye FE + arrays add karega
    .withColumn("tf_size", F.size("tfidf_arr"))
    .withColumn("nv_size", F.size("numeric_arr"))
)

# ===== prediction + final shaping ============================================
out = (
    base_for_pred
    .select(
        "ids","text","clean_text","ts","event_time","category","hasLink","origin","lang",
        "pred_label","p_neu","p_pos","err",
        F.coalesce(
            F.col("created_at_ts"),
            F.col("event_time").cast(T.TimestampType()),
            F.current_timestamp()
        ).alias("created_at"),
        F.current_timestamp().alias("processed_at"),
        "tf_size","nv_size"
    )
)
scored_df = (
    out
    .withColumn("tweet_id", F.col("ids"))                                 # <- consistent id for sinks
    .withColumn("need_pn", (F.col("p_neu") < F.lit(float(gate_t))))
    .withColumn(                                                         # <- 3-class hard label
        "final_label",
        F.when(F.col("p_neu") >= F.lit(float(gate_t)), F.lit(2))         # 2 = neutral
         .otherwise(F.col("pred_label"))                                 # 0=neg, 1=pos
    )
    .withColumn("sentiment_label", F.col("final_label").cast("int"))
    .withColumn(                                                         # <- human-readable
        "sentiment",
        F.when(F.col("sentiment_label") == 2, F.lit("neutral"))
         .when(F.col("sentiment_label") == 1, F.lit("positive"))
         .otherwise(F.lit("negative"))
    )
    .withColumn("sentiment_text", F.col("sentiment"))                    # <- keep if downstream expects this
    .withColumn("need_pn_dbg", F.col("need_pn").cast("int"))
    .withColumn("gate_t", F.lit(float(gate_t)))
    .withColumn("pn_t",   F.lit(float(pn_t)))
    .withColumn("origin", F.coalesce(F.col("origin"), F.lit("live")))
    .withColumn("source", F.lit("scorer"))
    .withColumn("type",   F.lit("tweet"))
#    .withColumn("lang",   F.coalesce(F.col("lang"), F.lit("und")))
)

print("\n[scorer] ==== SCHEMAS & COLS ====", flush=True)
print("[scorer] scored_df schema:", flush=True); scored_df.printSchema()
print("[scorer] ========================\n", flush=True)
from pyspark.sql.utils import StreamingQueryException

# ======================= foreachBatch sinks ==================================
unified_ckpt = f"{CKPT_BASE}/unified_v1"

MONGO_COLL_METRICS = os.environ.get("MONGO_COLL_METRICS", f"{MONGO_COLL}_metrics")

def write_to_sinks(batch_df, epoch_id: int):
    micro = None
    err_msgs = []
    try:
        micro = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        rows = micro.count()
        print(f"[foreachBatch] epoch={epoch_id} rows={rows}", flush=True)

        # -------------------- debug stats --------------------
        try:
            # --- label_dist WITHOUT toPandas ---
            dist_rows = (
                micro.groupBy("sentiment_label")
                     .count()
                     .orderBy("sentiment_label")
                     .collect()
            )
            print(
                "[dbg] label_dist:",
                [(r["sentiment_label"], r["count"]) for r in dist_rows],
                flush=True,
            )

            routed = micro.agg(F.avg(F.col("need_pn").cast("double")).alias("r")).collect()[0]["r"] or 0.0
            frac_neutral_gate = micro.agg(
                F.avg(F.when(F.col("p_neu") >= F.lit(float(gate_t)), 1.0).otherwise(0.0)).alias("f")
            ).collect()[0]["f"] or 0.0
            print(
                f"[dbg] routed_frac_to_PN={routed:.3f}  "
                f"frac_neutral_gate={frac_neutral_gate:.3f}  "
                f"gate_t={gate_t:.9f}  pn_t={pn_t}",
                flush=True,
            )

            pn_only = micro.filter(F.col("need_pn"))
            if pn_only.take(1):  # faster than limit().count()
                pn_stats = pn_only.agg(
                    F.expr("percentile_approx(p_pos, array(0.1,0.5,0.9))").alias("p_pos_pctls"),
                    F.expr("percentile_approx(p_neu, array(0.1,0.5,0.9))").alias("p_neu_pctls"),
                ).collect()[0]
                print(
                    "[dbg] PN-only pctls: p_pos", pn_stats["p_pos_pctls"],
                    " p_neu", pn_stats["p_neu_pctls"],
                    flush=True,
                )
            else:
                print("[dbg] PN-only pctls: (no PN rows this batch)", flush=True)

            pairs = (
                micro.groupBy("sentiment_label", "sentiment")
                .count()
                .orderBy("sentiment_label", "sentiment")
                .collect()
            )
            mapping_print = []
            for r in pairs:
    lbl = r["sentiment_label"]
    mapping_print.append((int(lbl) if lbl is not None else None,
                          r["sentiment"],
                          int(r["count"])))


print("[dbg] mapping check:", mapping_print, flush=True)

except Exception as e:
print("[dbg] metrics fail:", e, flush=True)
# -----------------------------------------------------

if rows == 0:
    return
 # ---- MINI DEBUG SAMPLE (safe: limit + collect) ----
        try:
            # prefer event_time, else processed_at
            order_col = F.coalesce(F.col("event_time"), F.col("processed_at"))
            sample_rows = (
                micro.select(
                        "ids", "lang", "category", "hasLink", "clean_text",
                        "need_pn", "final_label", "sentiment", "err",
                        "event_time", "processed_at"
                    )
                    .orderBy(order_col.desc())
                    .limit(10)
                    .collect()
            )
            print(
                "[dbg] sample:",
                [
                    (
                        d.get("ids"),
                        d.get("lang"),
                        d.get("category"),
                        d.get("hasLink"),
                        (d.get("clean_text") or "")[:60],
                        d.get("need_pn"),
                        d.get("sentiment"),
                    )
                    for d in (r.asDict(recursive=False) for r in sample_rows)
                ],
                flush=True,
            )
except Exception as e:
print("[dbg] sample ERROR:", repr(e), flush=True)
# ---------------------------------------------------

# -------------------- metrics row -------------------
has_sizes = ("tf_size" in micro.columns) and ("nv_size" in micro.columns)
gate_t_lit = F.lit(float(gate_t))
aggs = [F.count(F.lit(1)).alias("rows")]
if has_sizes:
    bad_cond = (F.col("tf_size") != F.lit(3000)) | (F.col("nv_size") != F.lit(21))
    aggs += [
        F.sum(F.when(bad_cond, 1).otherwise(0)).alias("bad_size_rows"),
        F.min("tf_size").alias("tf_min"), F.max("tf_size").alias("tf_max"),
        F.min("nv_size").alias("nv_min"), F.max("nv_size").alias("nv_max"),
    ]
if "p_neu" in micro.columns:
    aggs += [
        F.avg(F.when(F.col("p_neu") >= gate_t_lit, 1.0).otherwise(0.0)).alias("frac_neutral_gate"),
        F.expr("percentile_approx(p_neu, array(0.01,0.1,0.5,0.9,0.99))").alias("p_neu_pctls"),
    ]
metrics_df = (
    micro.agg(*aggs)
    .withColumn("batch_id", F.lit(int(epoch_id)))
    .withColumn("ts", F.current_timestamp())
)
# ----------------------------------------------------
# -------------------- Kafka OUT ---------------------
try:
    kafka_df = micro.selectExpr(
        "CAST(ids AS STRING) AS key",
        "to_json(named_struct("
        " 'tweet_id', ids, "
        " 'text', text, "
        " 'clean_text', clean_text, "
        " 'sentiment', sentiment, "
        " 'sentiment_label', sentiment_label, "
        " 'p_neu', p_neu, "
        " 'p_pos', p_pos, "
        " 'created_at', created_at, "
        " 'processed_at', processed_at, "
        " 'type', type, "
        " 'category', category, "
        " 'hasLink', hasLink, "
        " 'origin', origin, "
        " 'lang', lang, "
        " 'source', source "
        ")) AS value",
    )
    (kafka_df.write
     .format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("topic", OUT_TOPIC)
     .save())
except (Py4JJavaError, Exception) as e:

    msg = f"kafka_write:{type(e).__name__}:{str(e)[:200]}"
    print("[foreachBatch] ERROR", msg, flush=True)
    err_msgs.append(msg)
    # ----------------------------------------------------

    # ---- Mongo sinks (attempt regardless of Kafka result)
try:
    latest = (
        micro.withColumn("created_day", F.to_date("created_at"))
        .withColumn("processed_day", F.to_date("processed_at"))
        .withColumn("_id", F.col("ids"))
        .select(
            "_id", "ids", F.col("ids").alias("tweet_id"),
            "text", "clean_text",
            "sentiment", "sentiment_label", "p_neu", "p_pos",
            "created_at", "created_day", "processed_day",
            "category", "type", "origin", "lang", "source", "hasLink",
        )
        .where(F.col("_id").isNotNull() & (F.length("_id") > 0))
    )
    (latest.write
     .format("mongodb")
     .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
     .option("spark.mongodb.write.database", MONGO_DB)
     .option("spark.mongodb.write.collection", MONGO_COLL)
     .option("spark.mongodb.write.operationType", "replace")
     .mode("append")
     .save())
except (Py4JJavaError, Exception) as e:

    msg = f"mongo_latest:{type(e).__name__}:{str(e)[:200]}"
    print("[foreachBatch] ERROR", msg, flush=True)
    err_msgs.append(msg)

try:
    mongo_coll_hist = os.environ.get("MONGO_COLL_HISTORY", f"{MONGO_COLL}_history")
    hist = (
        micro.withColumn("created_day", F.to_date("created_at"))
        .withColumn("processed_day", F.to_date("processed_at"))
        .withColumn(
            "_id",
            F.concat_ws("_", F.col("ids"), F.date_format(F.col("created_day"), "yyyyMMdd")),
        )
        .select(
            "_id", "ids", F.col("ids").alias("tweet_id"),
            "text", "clean_text",
            "sentiment", "sentiment_label", "p_neu", "p_pos",
            "created_at", "created_day", "processed_day",
            "category", "type", "origin", "lang", "source", "hasLink",
        )
        .where(F.col("ids").isNotNull() & (F.length("ids") > 0))
    )
    (hist.write
     .format("mongodb")
     .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
     .option("spark.mongodb.write.database", MONGO_DB)
     .option("spark.mongodb.write.collection", mongo_coll_hist)
     .option("spark.mongodb.write.operationType", "replace")
     .mode("append")
     .save())
except (Py4JJavaError, Exception) as e:
    msg = f"mongo_hist:{type(e).__name__}:{str(e)[:200]}"
    print("[foreachBatch] ERROR", msg, flush=True)
    err_msgs.append(msg)

    # ---- metrics out (best-effort) ----
try:
    metrics_out = metrics_df if not err_msgs else metrics_df.withColumn("errors", F.lit(";".join(err_msgs)))
    (metrics_out.write
     .format("mongodb")
     .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
     .option("spark.mongodb.write.database", MONGO_DB)
     .option("spark.mongodb.write.collection", MONGO_COLL_METRICS)
     .mode("append")
     .save())
except (Py4JJavaError, Exception):
    pass

except Exception:
    print("[foreachBatch] FATAL:\n" + traceback.format_exc(), flush=True)
    raise
finally:
    if micro is not None:
        micro.unpersist()


spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")

main_query = (
    scored_df.writeStream
    .foreachBatch(write_to_sinks)
    .option("checkpointLocation", unified_ckpt)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print("[scorer] stream started:", main_query.id, flush=True)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    pass











BERT_BATCH=16

CUDA_VISIBLE_DEVICES=""
TF_CPP_MIN_LOG_LEVEL=2
TOKENIZERS_PARALLELISM=false


OMP_NUM_THREADS=1
OPENBLAS_NUM_THREADS=1
MKL_NUM_THREADS=1
NUMEXPR_NUM_THREADS=1
BERTWEET_ART_DIR="/home/ubuntu/app/sentiment_realtime_project/spark_jobs/bertweet_artifacts"
MALLOC_ARENA_MAX=2
PYTHONUNBUFFERED=1

# ---------- Mongo (Atlas) ----------
#MONGO_URI="mongodb+srv://Yasmeen_mh:Yasmeen_Azmat_Ali_123@cluster1.myhxac.mongodb.net/?retryWrites=true&w=majority"
MONGO_URI_BASE=mongodb+srv://Yasmeen_mh:wJ2uJZgY4dr3trXi@cluster1.myhxac.mongodb.net
MONGO_DB="twitter_rt"
MONGO_COLL="scored_tweets"

# ===== Kafka =====
KAFKA_BROKER=localhost:9092
KAFKA_IN_TOPIC=tweets
KAFKA_OUT_TOPIC=twitter_sentiment
ENABLE_KAFKA_SINK="true"
STARTING_OFFSETS=earliest
CKPT_BASE=/home/ubuntu/app/sentiment_realtime_project/checkpoints_v2

# Canonical names (code in-case in ko read kare)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
INPUT_TOPIC=tweets
OUTPUT_TOPIC=twitter_sentiment

IN_TOPIC=tweets
OUT_TOPIC=twitter_sentiment

# ===== Artifacts (exactly what you uploaded) =====
PN_ONNX_PATH=/home/ubuntu/app/sentiment_realtime_project/spark_jobs/bertweet_artifacts/pn_bertweet.onnx
HF_DIR=/home/ubuntu/app/sentiment_realtime_project/spark_jobs/bertweet_artifacts/hf
GATE_PKL=/home/ubuntu/app/sentiment_realtime_project/spark_jobs/bertweet_artifacts/gate_xgb.pkl
THRESHOLDS_JSON=/home/ubuntu/app/sentiment_realtime_project/spark_jobs/bertweet_artifacts/thresholds_and_config.json

# ===== Classic FE models (unchanged) =====
TFIDF_MODEL_PATH=/home/ubuntu/app/sentiment_realtime_project/spark_jobs/tfidf_model_shared
SCALER_MODEL_PATH=/home/ubuntu/app/sentiment_realtime_project/spark_jobs/minmax_scaler_model_shared

# ===== Embeddings used in training =====
SENTENCE_MODEL_NAME=all-MiniLM-L6-v2

# ===== Tunables (meta has these, env only used as fallback) =====
GATE_T=0.90
PN_T=0.40
GATE_NEU_CLASS=1
MAX_LEN=224
















































# ================= TRAINING-PARITY SCORER (no ONNX) =================
import json, string, re, pandas as pd
import nltk
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import pandas_udf, udf, col
from pyspark.storagelevel import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler, MinMaxScalerModel, StandardScalerModel
from pyspark.ml.functions import vector_to_array
from pyspark.ml.linalg import VectorUDT
from typing import List

# --- NLTK resources (download once on driver; executors share site-packages) ---
for pkg in ["stopwords","vader_lexicon","opinion_lexicon"]:
    try: nltk.data.find(f"corpora/{pkg}")
    except LookupError: nltk.download(pkg)

from nltk.corpus import stopwords, opinion_lexicon
from nltk.stem import PorterStemmer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# ===== Artefact paths =====
WORD_TFIDF_PATH = os.getenv("WORD_TFIDF_PATH", "tfidf_model_shared")
CHAR_TFIDF_PATH = os.getenv("CHAR_TFIDF_PATH", "tfidf_char_model_shared")
NUM_SCALER_PATH = os.getenv("NUM_SCALER_PATH", "numeric_scaler_model_shared_v2")
META_JSON       = os.getenv("META_JSON", "artifacts_meta/meta_vectorizers.json")
CLASSIFIER_PATH = os.getenv("CLASSIFIER_PATH")  # optional

# Labels (training side)
LABEL_ORDER = [s.strip().lower() for s in os.getenv("LABEL_ORDER","negative,neutral,positive").split(",")]
LBL2IDX = {s:i for i,s in enumerate(LABEL_ORDER)}

# Dashboard numeric codes (output contract): 1=pos, 2=neu, 0=neg
OUT_CODE = {"positive":2, "neutral":1, "negative":0}

# ===== Load vectorizers/scaler =====
tfidf_word_model = PipelineModel.load(WORD_TFIDF_PATH)
tfidf_char_model = PipelineModel.load(CHAR_TFIDF_PATH)

# scaler model type detection
_scaler_model = None
try:
    _scaler_model = MinMaxScalerModel.load(NUM_SCALER_PATH)
except Exception:
    try:
        _scaler_model = StandardScalerModel.load(NUM_SCALER_PATH)
    except Exception:
        raise RuntimeError(f"Scaler model not found at {NUM_SCALER_PATH}")

# ===== NUMERIC_ORDER (read from meta if possible) =====
DEFAULT_NUMERIC_ORDER = [
    "vader_score","vader_bucket",
    "hour","day_of_week","is_weekend",
    "text_length","word_count","char_density",
    "has_mentions","has_hashtags","has_links","is_question",
    "sentiment_keyword_count","negation_count","capital_word_count","emoji_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
    "lang_en","lang_arabic","lang_indic","lang_cyril","lang_cjk","lang_latin_other",
    "emoji_polarity",
]
try:
    with open(META_JSON,"r") as _f:
        _meta = json.load(_f)
        NUMERIC_ORDER = _meta.get("numeric_order", DEFAULT_NUMERIC_ORDER)
except Exception:
    NUMERIC_ORDER = DEFAULT_NUMERIC_ORDER

# ===== Cleaning (training parity) =====
URL_RE     = re.compile(r"http\S+|www\.\S+")
MENTION_RE = re.compile(r"@\w+")
@F.udf(T.StringType())
def clean_text_udf(t: str) -> str:
    if not t: return ""
    txt = URL_RE.sub(" ", str(t))
    txt = MENTION_RE.sub(" ", txt)
    txt = txt.replace("&amp;","&")
    # keep non-ASCII (multilingual), strip punct to space
    txt = re.sub("["+re.escape(string.punctuation)+"]", " ", txt)
    txt = re.sub(r"(.)\1{2,}", r"\1\1", txt)
    txt = re.sub(r"\s+"," ", txt).strip().lower()
    return txt

# ===== Language detect (fast heuristics + langid) =====
import langid
ARABIC_RE      = re.compile(r'[\u0600-\u06FF]')
DEVANAGARI_RE  = re.compile(r'[\u0900-\u097F]')
CYRILLIC_RE    = re.compile(r'[\u0400-\u04FF]')
CJK_RE         = re.compile(r'[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]')
langid.set_languages(['en','ar','ur','fa','hi','bn','tr','es','fr','de','ru','zh','ja','ko'])

def ascii_ratio(s: str) -> float:
    if not s: return 1.0
    a = sum(1 for ch in s if ord(ch) < 128)
    return a / max(1, len(s))

def detect_one(text: str) -> str:
    if not text: return 'unknown'
    if ARABIC_RE.search(text):     return 'ar'
    if DEVANAGARI_RE.search(text): return 'hi'
    if CYRILLIC_RE.search(text):   return 'ru'
    if CJK_RE.search(text):        return 'zh'
    lang, score = langid.classify(text)
    if lang == 'en' and ascii_ratio(text) >= 0.95 and score > -20:
        return 'en'
    if len(text.split()) < 2 or len(text) < 6:
        return 'unknown'
    return lang

@pandas_udf(T.StringType())
def detect_lang_pd(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).apply(detect_one)

def lang_group_expr(col_lang):
    return (F.when(col_lang=="en","en")
              .when(col_lang.isin("ar","fa","ur"), "arabic")
              .when(col_lang.isin("hi","bn"),      "indic")
              .when(col_lang=="ru",                "cyril")
              .when(col_lang.isin("zh","ja","ko"), "cjk")
              .otherwise("latin_other"))

LANG_GROUPS = ["en","arabic","indic","cyril","cjk","latin_other"]

# ===== Lexicons / counters =====
stop_words_bv = spark.sparkContext.broadcast({w.lower() for w in stopwords.words("english")})
ps = PorterStemmer()
sentiment_keywords = {"love","hate","amazing","awful","great","bad","good","terrible","worst","excellent"}
negation_words     = {"not","no","never","none","nobody","isn't","wasn't","aren't","don't","didn't","won't","can't"}
positive_lexicon   = {"good","great","excellent","amazing","love","nice","happy","best"}
negative_lexicon   = {"bad","terrible","awful","hate","worst","sad","horrible","angry"}
extended_positive  = set(opinion_lexicon.positive())
extended_negative  = set(opinion_lexicon.negative())

neg_bv  = spark.sparkContext.broadcast(negation_words)
kw_bv   = spark.sparkContext.broadcast(sentiment_keywords)
pos_bv  = spark.sparkContext.broadcast(positive_lexicon)
neg2_bv = spark.sparkContext.broadcast(negative_lexicon)
ep_bv   = spark.sparkContext.broadcast(extended_positive)
en_bv   = spark.sparkContext.broadcast(extended_negative)

@udf(T.IntegerType())
def count_negations(text):
    if text:
        words = text.lower().split()
        return sum(1 for w in words if w in neg_bv.value)
    return 0

@udf(T.IntegerType())
def count_sentiment_keywords_cond(text, lang):
    if text and lang == 'en':
        words = text.lower().split()
        return sum(1 for w in words if w in kw_bv.value)
    return 0

@udf(T.IntegerType())
def count_positive_words_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in pos_bv.value)
    return 0

@udf(T.IntegerType())
def count_negative_words_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in neg2_bv.value)
    return 0

@udf(T.IntegerType())
def count_extended_positive_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in ep_bv.value)
    return 0

@udf(T.IntegerType())
def count_extended_negative_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in en_bv.value)
    return 0

@udf(T.IntegerType())
def count_capital_words(text):
    if text: return len(re.findall(r"\b[A-Z]{2,}\b", text))
    return 0

EMOJI_RE    = re.compile(r'[\U0001F300-\U0001FAFF\U00002600-\U000026FF]')
EMOTICON_RE = re.compile(r'(?:(?:[:;=8][\-^]?[)DpP/\\|oO\(\]])|<3|:\'\(|xD|XD)')

@udf(T.IntegerType())
def count_emoji_like(text):
    if not text: return 0
    return len(EMOJI_RE.findall(text)) + len(EMOTICON_RE.findall(text))

pos_emoji = set(list("😀😁😂🤣😊😍😘😎🙂☺️👍💖✨🎉🔥💯😺"))
neg_emoji = set(list("😞😠😡🤬😢😭☹️👎💔💀😒🙄😕😤"))

@udf(T.IntegerType())
def emoji_polarity(text):
    if not text: return 0
    p = sum(1 for ch in text if ch in pos_emoji)
    n = sum(1 for ch in text if ch in neg_emoji)
    val = p - n
    return 5 if val>5 else (-5 if val<-5 else val)

@udf(T.IntegerType())
def count_punctuation(text):
    return len(re.findall(r"[{}]".format(re.escape(string.punctuation)), text)) if text else 0

# VADER (EN only) as pandas UDF (one SIA per worker)
@pandas_udf(T.FloatType())
def vader_polarity_pd(series: pd.Series) -> pd.Series:
    state = getattr(vader_polarity_pd, "_state", None)
    if state is None:
        vader_polarity_pd._state = {"sia": SentimentIntensityAnalyzer()}
        state = vader_polarity_pd._state
    sia = state["sia"]
    vals = series.fillna("").astype(str)
    return vals.apply(lambda t: float(sia.polarity_scores(t)["compound"]) if t else 0.0).astype(float)

# Char n-grams (for char TF-IDF)
from pyspark.sql.types import ArrayType, StringType
@udf(ArrayType(StringType()))
def make_char_ngrams(s):
    s = (s or "")
    s = re.sub(r"\s+", " ", s).strip()
    out = []
    L = len(s)
    for n in range(3, 6):  # 3..5
        if L >= n:
            out.extend([s[i:i+n] for i in range(L-n+1)])
    return out

# ===== Classifier loader (optional) =====
def _try_load_classifier(path: str):
    if not path: return None
    from pyspark.ml.classification import (
        LogisticRegressionModel, RandomForestClassificationModel,
        GBTClassificationModel, LinearSVCModel, NaiveBayesModel
    )
    loaders = [
        LogisticRegressionModel.load,
        RandomForestClassificationModel.load,
        GBTClassificationModel.load,
        NaiveBayesModel.load,
        # LinearSVC has no probability; we can still use decisionFunction later
        LinearSVCModel.load,
    ]
    for L in loaders:
        try:
            return L(path)
        except Exception:
            continue
    # As a last resort, try generic PipelineModel (if classifier wrapped)
    try:
        return PipelineModel.load(path)
    except Exception:
        print(f"[warn] Could not load classifier from {path}")
        return None

clf_model = _try_load_classifier(CLASSIFIER_PATH)

# ===== Build streaming features =====
def build_features(df):
    # 0) base cleaning + event time
    df = (df
          .withColumn("clean_text", clean_text_udf(F.col("text")))
          .filter(F.length("clean_text") > 0)
          .withColumn("created_at_ts",
              F.coalesce(F.to_timestamp(F.from_unixtime(F.col("created_at")/1000.0)),
                         F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0))))
          .withColumn("event_time",
              F.coalesce(F.col("created_at_ts"), F.current_timestamp()))
          )

    # 1) language
    df = df.withColumn("lang", detect_lang_pd(F.col("clean_text")))
    df = df.withColumn("lang_group", lang_group_expr(F.col("lang")))
    for g in ["en","arabic","indic","cyril","cjk","latin_other"]:
        df = df.withColumn(f"lang_{g}", F.when(F.col("lang_group")==g, 1).otherwise(0))

    # 2) temporal
    df = (df
          .withColumn("hour",        F.hour("event_time"))
          .withColumn("day_of_week", F.dayofweek("event_time"))
          .withColumn("is_weekend",  F.when(F.col("day_of_week").isin(1,7), 1).otherwise(0))
          )

    # 3) metrics & flags
    df = (df
          .withColumn("text_length", F.length("clean_text"))
          .withColumn("word_count",
              F.when(F.col("clean_text")== "", 0).otherwise(F.size(F.split("clean_text"," "))))
          .withColumn("char_density",
              F.when(F.col("word_count")==0, 0.0)
               .otherwise((F.col("text_length")/F.col("word_count")).cast(T.FloatType())))
          .withColumn("char_density", F.when(F.col("char_density")>20.0, 20.0).otherwise(F.col("char_density")))
          .withColumn("has_mentions", F.when(F.col("text").rlike(r'(^|\s)@\w+'), 1).otherwise(0))
          .withColumn("has_hashtags", F.when(F.col("text").rlike(r'(^|\s)#\w+'), 1).otherwise(0))
          .withColumn("has_links",    F.when(F.col("text").rlike(r'http[s]?://|www\.'), 1).otherwise(0))
          .withColumn("is_question",  F.when(F.col("text").rlike(r'\?\s*$'), 1).otherwise(0))
          .withColumn("negation_count",          count_negations(F.col("clean_text")))
          .withColumn("capital_word_count",      count_capital_words(F.col("text")))
          .withColumn("emoji_count",             F.least(count_emoji_like(F.col("text")), F.lit(5)))
          .withColumn("sentiment_keyword_count", count_sentiment_keywords_cond(F.col("clean_text"), F.col("lang")))
          .withColumn("positive_lexicon_count",  count_positive_words_cond(F.col("clean_text"),  F.col("lang")))
          .withColumn("negative_lexicon_count",  count_negative_words_cond(F.col("clean_text"),  F.col("lang")))
          .withColumn("extended_positive_lexicon_count", count_extended_positive_cond(F.col("clean_text"), F.col("lang")))
          .withColumn("extended_negative_lexicon_count", count_extended_negative_cond(F.col("clean_text"), F.col("lang")))
          .withColumn("emoji_polarity",          emoji_polarity(F.col("text")))
          .withColumn("punctuation_count_raw",   count_punctuation(F.col("text")))
          .withColumn("punctuation_count",       F.least(F.col("punctuation_count_raw"), F.lit(30)))
          .drop("punctuation_count_raw")
          )

    # 4) Vader for EN only
    en = (df.filter(F.col("lang")=="en")
            .select("ids","clean_text")
            .withColumn("vader_score", vader_polarity_pd(F.col("clean_text")))
            .withColumn("vader_bucket",
                F.when(F.col("vader_score")<=-0.4, F.lit(0))
                 .when(F.col("vader_score")>= 0.4, F.lit(1))
                 .otherwise(F.lit(2))))
    df = (df.join(en.select("ids","vader_score","vader_bucket"), on="ids", how="left")
            .fillna({"vader_score":0.0, "vader_bucket":2}))

    # 5) word & char TF-IDF
    d1 = tfidf_word_model.transform(df)                               # -> tfidf_l2
    d2 = d1.withColumn("char_ngrams", make_char_ngrams(F.col("clean_text")))
    d3 = tfidf_char_model.transform(d2)                               # -> char_tfidf_l2

    # 6) numeric vector + scale
    missing = [c for c in NUMERIC_ORDER if c not in d3.columns]
    if missing:
        raise RuntimeError(f"Missing numeric columns: {missing}")
    num_assembler = VectorAssembler(inputCols=NUMERIC_ORDER, outputCol="numeric_raw", handleInvalid="keep")
    d4 = num_assembler.transform(d3)
    d5 = _scaler_model.transform(d4)                                   # -> numeric_scaled

    # 7) final features (same concat order as training)
    final_assembler = VectorAssembler(
        inputCols=["tfidf_l2","char_tfidf_l2","numeric_scaled"],
        outputCol="features_final",
        handleInvalid="keep"
    )
    out = final_assembler.transform(d5)
    return out

# ===== Prediction head (classifier or fallback) =====
def add_predictions(df):
    if clf_model is None:
        # Fallback: english → vader_bucket (0=neg,1=pos,2=neu); else emoji_polarity sign
        pred = (F.when(F.col("lang")=="en",
                       F.when(F.col("vader_bucket")==0, F.lit("negative"))
                        .when(F.col("vader_bucket")==1, F.lit("positive"))
                        .otherwise(F.lit("neutral")))
                  .otherwise(
                       F.when(F.col("emoji_polarity")<0,  F.lit("negative"))
                        .when(F.col("emoji_polarity")>0,  F.lit("positive"))
                        .otherwise(F.lit("neutral"))
                  ))
        df2 = df.withColumn("sentiment", pred)
        # crude probs: 0.8 for chosen class, 0.1 others
        idx = F.when(F.col("sentiment")=="negative", F.lit(LBL2IDX.get("negative",0))) \
               .when(F.col("sentiment")=="neutral",  F.lit(LBL2IDX.get("neutral",1))) \
               .otherwise(F.lit(LBL2IDX.get("positive",2)))
        # 3-length array with chosen high prob
        def _prob_expr(i):
            arr = [F.lit(0.1), F.lit(0.1), F.lit(0.1)]
            pos = [LBL2IDX.get("negative",0), LBL2IDX.get("neutral",1), LBL2IDX.get("positive",2)]
            return F.array(*[F.when(idx==p, F.lit(0.8)).otherwise(v) for p,v in zip(pos,arr)])
        df2 = df2.withColumn("probs_arr", _prob_expr(idx))
    else:
        scored = clf_model.transform(df)
        # probability vector → python list
        if "probability" in scored.columns:
            df2 = scored.withColumn("probs_arr", vector_to_array("probability"))
            # predicted label index
            df2 = df2.withColumn("pred_idx",
                    F.expr(f"aggregate(sequence(0, size(probs_arr)-1), named_struct('i',0,'v',-1.0), "
                           f"(acc,x) -> case when probs_arr[x] > acc.v then named_struct('i',x,'v',probs_arr[x]) else acc end).i"))
        else:
            # e.g., LinearSVC: no probability, use prediction as index
            df2 = scored.withColumn("probs_arr", F.array(F.lit(0.0),F.lit(0.0),F.lit(0.0))) \
                        .withColumn("pred_idx", F.col("prediction").cast("int"))
        # map pred_idx (0/1/2 per training order) → label text
        idx2lbl = F.create_map([F.lit(i) for kv in sum(([i, LABEL_ORDER[i]] for i in range(len(LABEL_ORDER))), [])])
        df2 = df2.withColumn("sentiment", idx2lbl[F.col("pred_idx")])

    # Standardize outputs
    out = (df2
           .withColumn("sentiment_text", F.col("sentiment"))
           .withColumn("sentiment_label", F.when(F.col("sentiment_text")=="positive", F.lit(1))
                                          .when(F.col("sentiment_text")=="neutral",  F.lit(2))
                                          .otherwise(F.lit(0)))
           # convenience fields
           .withColumn("p_pos", F.when(F.size("probs_arr")>=3, F.element_at("probs_arr", LBL2IDX.get("positive",2)+1)).otherwise(F.lit(None)))
           .withColumn("p_neu", F.when(F.size("probs_arr")>=3, F.element_at("probs_arr", LBL2IDX.get("neutral",1)+1)).otherwise(F.lit(None)))
           )
    return out

# ================= END TRAINING-PARITY BLOCK =================


















#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, math, time, re, string, traceback
from typing import List, Tuple

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import pandas_udf, udf, col
from pyspark import StorageLevel
from pyspark.sql.utils import StreamingQueryException
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array

# ============== Spark session ==============
spark = (
    SparkSession.builder
        .appName("twitter-scorer-hier")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "256")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ============== Kafka / topics / checkpoints ==============
BOOTSTRAP = (
    os.environ.get("KAFKA_BOOTSTRAP")
    or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    or "localhost:9092"
)
IN_TOPIC  = os.environ.get("KAFKA_IN_TOPIC",  os.environ.get("IN_TOPIC", "tweets"))
OUT_TOPIC = os.environ.get("KAFKA_OUT_TOPIC", os.environ.get("OUT_TOPIC", "twitter_sentiment"))
CHECKPOINT_DIR = os.path.expanduser(
    os.environ.get("CHECKPOINT_DIR", "/home/ubuntu/app/sentiment_realtime_project/checkpoints_xlmr_hier")
)
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "latest")

# ============== Feature vectorizer artifacts ==============
WORD_TFIDF_PATH = os.getenv("WORD_TFIDF_PATH", "tfidf_model_shared")
CHAR_TFIDF_PATH = os.getenv("CHAR_TFIDF_PATH", "tfidf_char_model_shared")
NUM_SCALER_PATH = os.getenv("NUM_SCALER_PATH", "numeric_scaler_model_shared_v2")
META_JSON       = os.getenv("META_JSON", "artifacts_meta/meta_vectorizers.json")

# ============== Hierarchical artifacts dir ==============
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "hier_bertweet_xgb_15p_artifacts").rstrip("/")

# ============== Mongo (optional) ==============
MONGO_URI_BASE     = os.getenv("MONGO_URI_BASE", "").strip()
MONGO_DB           = os.getenv("MONGO_DB", "twitter_rt")
MONGO_COLL         = os.getenv("MONGO_COLL", "scored_tweets")
MONGO_COLL_HISTORY = os.getenv("MONGO_COLL_HISTORY", f"{MONGO_COLL}_history")
MONGO_COLL_METRICS = os.getenv("MONGO_COLL_METRICS", f"{MONGO_COLL}_metrics")
USE_MONGO = bool(MONGO_URI_BASE)

# ============== Load vectorizers/scaler ==============
from pyspark.ml.feature import MinMaxScalerModel, StandardScalerModel
try:
    scaler_model = MinMaxScalerModel.load(NUM_SCALER_PATH)
except Exception:
    scaler_model = StandardScalerModel.load(NUM_SCALER_PATH)

tfidf_word_model = PipelineModel.load(WORD_TFIDF_PATH)
tfidf_char_model = PipelineModel.load(CHAR_TFIDF_PATH)

# numeric order (prefer meta)
DEFAULT_NUMERIC_ORDER = [
    "vader_score","vader_bucket",
    "hour","day_of_week","is_weekend",
    "text_length","word_count","char_density",
    "has_mentions","has_hashtags","has_links","is_question",
    "sentiment_keyword_count","negation_count","capital_word_count","emoji_count",
    "positive_lexicon_count","negative_lexicon_count",
    "punctuation_count",
    "extended_positive_lexicon_count","extended_negative_lexicon_count",
    "lang_en","lang_arabic","lang_indic","lang_cyril","lang_cjk","lang_latin_other",
    "emoji_polarity"
]
try:
    with open(META_JSON,"r") as _f:
        NUMERIC_ORDER = json.load(_f).get("numeric_order", DEFAULT_NUMERIC_ORDER)
except Exception:
    NUMERIC_ORDER = DEFAULT_NUMERIC_ORDER

# ============== Load hierarchical components ==============
import joblib
import numpy as np

# Gate + isotonic
GATE_PATH = os.path.join(ARTIFACTS_DIR, "gate_xgb.pkl")
ISO_PATH  = os.path.join(ARTIFACTS_DIR, "gate_isotonic.pkl")
gate      = joblib.load(GATE_PATH)
iso       = joblib.load(ISO_PATH)

# PN XGB + blender
PN_XGB_PATH   = os.path.join(ARTIFACTS_DIR, "pn_xgb.pkl")
PN_BLEND_PATH = os.path.join(ARTIFACTS_DIR, "pn_blend_lr.pkl")
xgb_pn        = joblib.load(PN_XGB_PATH)
pn_blend      = joblib.load(PN_BLEND_PATH)

# PN BERTweet (ONNX) + tokenizer + temperature
from transformers import AutoTokenizer
import onnxruntime as ort
MAX_LEN = 192
MODEL_NAME = "vinai/bertweet-base"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, use_fast=False, normalization=True)
tokenizer.model_max_length = MAX_LEN
tokenizer.truncation_side = "right"
tokenizer.padding_side = "right"
PAD_ID = tokenizer.pad_token_id if tokenizer.pad_token_id is not None else 1

ONNX_PATH = os.path.join(ARTIFACTS_DIR, "pn_bertweet.onnx")
sess_opts = ort.SessionOptions()
sess_opts.intra_op_num_threads = 1
sess_opts.inter_op_num_threads = 1
sess_opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
onnx_sess = ort.InferenceSession(ONNX_PATH, sess_options=sess_opts, providers=["CPUExecutionProvider"])

with open(os.path.join(ARTIFACTS_DIR, "pn_temperature.json"), "r") as f:
    T_STAR = float(json.load(f)["temperature"])

# Thresholds / config
CFG_PATH = os.path.join(ARTIFACTS_DIR, "thresholds_and_config.json")
with open(CFG_PATH, "r") as f:
    THR = json.load(f)
T_NEU         = float(THR["t_neu"])
T_POS         = float(THR["t_pos"])
PN_FLOOR      = float(THR.get("pn_floor", 0.25))
T_CONF        = float(THR.get("t_conf", 0.65))
MARGIN        = float(THR.get("margin", 0.08))
NEUTRAL_DELTA = float(THR.get("neutral_delta", 0.06))
LABEL_ORDER   = THR.get("label_order", ["negative","neutral","positive"])

print(f"✅ Loaded artifacts from {ARTIFACTS_DIR}")
print(f"   thresholds: t_neu={T_NEU} t_pos={T_POS} pn_floor={PN_FLOOR} t_conf={T_CONF} margin={MARGIN} neutral_delta={NEUTRAL_DELTA}")
print(f"   vectorizers: word='{WORD_TFIDF_PATH}', char='{CHAR_TFIDF_PATH}', scaler='{NUM_SCALER_PATH}'")
print(f"   numeric_order({len(NUMERIC_ORDER)}):", NUMERIC_ORDER)

# ============== NLTK + helpers ==============
import nltk
for pkg in ["stopwords","vader_lexicon","opinion_lexicon"]:
    try: nltk.data.find(f"corpora/{pkg}")
    except LookupError: nltk.download(pkg)

from nltk.corpus import stopwords, opinion_lexicon
from nltk.stem import PorterStemmer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

stop_words_bv = spark.sparkContext.broadcast({w.lower() for w in stopwords.words("english")})
ps = PorterStemmer()

# Cleaning (training-parity)
URL_RE     = re.compile(r"http\S+|www\.\S+")
MENTION_RE = re.compile(r"@\w+")
@F.udf(T.StringType())
def clean_text_udf(t: str) -> str:
    if not t: return ""
    txt = URL_RE.sub(" ", str(t))
    txt = MENTION_RE.sub(" ", txt)
    txt = txt.replace("&amp;","&")
    txt = re.sub("["+re.escape(string.punctuation)+"]", " ", txt)
    txt = re.sub(r"(.)\1{2,}", r"\1\1", txt)
    txt = re.sub(r"\s+"," ", txt).strip().lower()
    return txt

# Language detect (same heuristics as training)
import langid
ARABIC_RE      = re.compile(r'[\u0600-\u06FF]')
DEVANAGARI_RE  = re.compile(r'[\u0900-\u097F]')
CYRILLIC_RE    = re.compile(r'[\u0400-\u04FF]')
CJK_RE         = re.compile(r'[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]')
langid.set_languages(['en','ar','ur','fa','hi','bn','tr','es','fr','de','ru','zh','ja','ko'])

def ascii_ratio(s: str) -> float:
    if not s: return 1.0
    a = sum(1 for ch in s if ord(ch) < 128)
    return a / max(1, len(s))

def detect_one(text: str) -> str:
    if not text: return 'unknown'
    if ARABIC_RE.search(text):     return 'ar'
    if DEVANAGARI_RE.search(text): return 'hi'
    if CYRILLIC_RE.search(text):   return 'ru'
    if CJK_RE.search(text):        return 'zh'
    lang, score = langid.classify(text)
    if lang == 'en' and ascii_ratio(text) >= 0.95 and score > -20:
        return 'en'
    if len(text.split()) < 2 or len(text) < 6:
        return 'unknown'
    return lang

@pandas_udf(T.StringType())
def detect_lang_pd(s: 'pd.Series') -> 'pd.Series':
    import pandas as pd
    return s.fillna("").astype(str).apply(detect_one)

def lang_group_expr(col_lang):
    return (F.when(col_lang=="en","en")
              .when(col_lang.isin("ar","fa","ur"), "arabic")
              .when(col_lang.isin("hi","bn"),      "indic")
              .when(col_lang=="ru",                "cyril")
              .when(col_lang.isin("zh","ja","ko"), "cjk")
              .otherwise("latin_other"))

LANG_GROUPS = ["en","arabic","indic","cyril","cjk","latin_other"]

# Lexicons / counters
negation_words     = {"not","no","never","none","nobody","isn't","wasn't","aren't","don't","didn't","won't","can't"}
sentiment_keywords = {"love","hate","amazing","awful","great","bad","good","terrible","worst","excellent"}
positive_lexicon   = {"good","great","excellent","amazing","love","nice","happy","best"}
negative_lexicon   = {"bad","terrible","awful","hate","worst","sad","horrible","angry"}
extended_positive  = set(opinion_lexicon.positive())
extended_negative  = set(opinion_lexicon.negative())

neg_bv  = spark.sparkContext.broadcast(negation_words)
kw_bv   = spark.sparkContext.broadcast(sentiment_keywords)
pos_bv  = spark.sparkContext.broadcast(positive_lexicon)
neg2_bv = spark.sparkContext.broadcast(negative_lexicon)
ep_bv   = spark.sparkContext.broadcast(extended_positive)
en_bv   = spark.sparkContext.broadcast(extended_negative)

@udf(T.IntegerType())
def count_negations(text):
    if text:
        words = text.lower().split()
        return sum(1 for w in words if w in neg_bv.value)
    return 0

@udf(T.IntegerType())
def count_sentiment_keywords_cond(text, lang):
    if text and lang == 'en':
        words = text.lower().split()
        return sum(1 for w in words if w in kw_bv.value)
    return 0

@udf(T.IntegerType())
def count_positive_words_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in pos_bv.value)
    return 0

@udf(T.IntegerType())
def count_negative_words_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in neg2_bv.value)
    return 0

@udf(T.IntegerType())
def count_extended_positive_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in ep_bv.value)
    return 0

@udf(T.IntegerType())
def count_extended_negative_cond(text, lang):
    if text and lang == 'en':
        return sum(1 for w in text.lower().split() if w in en_bv.value)
    return 0

@udf(T.IntegerType())
def count_capital_words(text):
    if text: return len(re.findall(r"\b[A-Z]{2,}\b", text))
    return 0

EMOJI_RE    = re.compile(r'[\U0001F300-\U0001FAFF\U00002600-\U000026FF]')
EMOTICON_RE = re.compile(r'(?:(?:[:;=8][\-^]?[)DpP/\\|oO\(\]])|<3|:\'\(|xD|XD)')

@udf(T.IntegerType())
def count_emoji_like(text):
    if not text: return 0
    return len(EMOJI_RE.findall(text)) + len(EMOTICON_RE.findall(text))

pos_emoji = set(list("😀😁😂🤣😊😍😘😎🙂☺️👍💖✨🎉🔥💯😺"))
neg_emoji = set(list("😞😠😡🤬😢😭☹️👎💔💀😒🙄😕😤"))

@udf(T.IntegerType())
def emoji_polarity(text):
    if not text: return 0
    p = sum(1 for ch in text if ch in pos_emoji)
    n = sum(1 for ch in text if ch in neg_emoji)
    val = p - n
    return 5 if val>5 else (-5 if val<-5 else val)

@udf(T.IntegerType())
def count_punctuation(text):
    return len(re.findall(r"[{}]".format(re.escape(string.punctuation)), text)) if text else 0

# VADER (EN only) – one SIA per worker
@pandas_udf(T.FloatType())
def vader_polarity_pd(series: 'pd.Series') -> 'pd.Series':
    import pandas as pd
    state = getattr(vader_polarity_pd, "_state", None)
    if state is None:
        from nltk.sentiment.vader import SentimentIntensityAnalyzer
        vader_polarity_pd._state = {"sia": SentimentIntensityAnalyzer()}
        state = vader_polarity_pd._state
    sia = state["sia"]
    vals = series.fillna("").astype(str)
    return vals.apply(lambda t: float(sia.polarity_scores(t)["compound"]) if t else 0.0).astype(float)

# Char n-grams for char TFIDF
from pyspark.sql.types import ArrayType, StringType
@udf(ArrayType(StringType()))
def make_char_ngrams(s):
    s = (s or "")
    s = re.sub(r"\s+", " ", s).strip()
    out = []
    L = len(s)
    for n in range(3, 6):
        if L >= n:
            out.extend([s[i:i+n] for i in range(L-n+1)])
    return out

# ============== Read Kafka stream ==============
value_schema = T.StructType([
    T.StructField("ids", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("ts", T.LongType()),
    T.StructField("created_at", T.LongType()),
    T.StructField("category", T.StringType()),
    T.StructField("hasLink", T.BooleanType()),
    T.StructField("origin", T.StringType()),
    T.StructField("lang", T.StringType()),
])

raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", BOOTSTRAP)
         .option("subscribe", IN_TOPIC)
         .option("startingOffsets", STARTING_OFFSETS)
         .option("maxOffsetsPerTrigger", "2000")
         .load()
)

parsed = (raw
    .select(F.col("value").cast("string").alias("v"))
    .select(F.from_json("v", value_schema, {"mode": "PERMISSIVE"}).alias("j"))
    .select("j.*")
    .where(F.col("ids").isNotNull() & F.col("text").isNotNull())
    .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("ts")/1000.0)))
    .withWatermark("event_time", "1 day")
    .dropDuplicates(["ids"])
)

scorable = parsed.coalesce(2)

# ============== Build features (training parity) ==============
def build_features(df):
    df = (df
        .withColumn("clean_text", clean_text_udf(F.col("text")))
        .filter(F.length("clean_text") > 0)
        .withColumn("created_at_ts",
            F.coalesce(F.to_timestamp(F.from_unixtime(F.col("created_at")/1000.0)),
                       F.col("event_time")))
    )

    # language
    df = df.withColumn("lang", detect_lang_pd(F.col("clean_text")))
    df = df.withColumn("lang_group", lang_group_expr(F.col("lang")))
    for g in LANG_GROUPS:
        df = df.withColumn(f"lang_{g}", F.when(F.col("lang_group")==g, 1).otherwise(0))

    # temporal
    df = (df
        .withColumn("hour",        F.hour("created_at_ts"))
        .withColumn("day_of_week", F.dayofweek("created_at_ts"))
        .withColumn("is_weekend",  F.when(F.col("day_of_week").isin(1,7), 1).otherwise(0))
    )

    # metrics & flags
    df = (df
        .withColumn("text_length", F.length("clean_text"))
        .withColumn("word_count",
            F.when(F.col("clean_text")== "", 0).otherwise(F.size(F.split("clean_text"," "))))
        .withColumn("char_density",
            F.when(F.col("word_count")==0, 0.0)
             .otherwise((F.col("text_length")/F.col("word_count")).cast(T.FloatType())))
        .withColumn("char_density", F.when(F.col("char_density")>20.0, 20.0).otherwise(F.col("char_density")))
        .withColumn("has_mentions", F.when(F.col("text").rlike(r'(^|\s)@\w+'), 1).otherwise(0))
        .withColumn("has_hashtags", F.when(F.col("text").rlike(r'(^|\s)#\w+'), 1).otherwise(0))
        .withColumn("has_links",    F.when(F.col("text").rlike(r'http[s]?://|www\.'), 1).otherwise(0))
        .withColumn("is_question",  F.when(F.col("text").rlike(r'\?\s*$'), 1).otherwise(0))
        .withColumn("negation_count",          count_negations(F.col("clean_text")))
        .withColumn("capital_word_count",      count_capital_words(F.col("text")))
        .withColumn("emoji_count",             F.least(count_emoji_like(F.col("text")), F.lit(5)))
        .withColumn("sentiment_keyword_count", count_sentiment_keywords_cond(F.col("clean_text"), F.col("lang")))
        .withColumn("positive_lexicon_count",  count_positive_words_cond(F.col("clean_text"),  F.col("lang")))
        .withColumn("negative_lexicon_count",  count_negative_words_cond(F.col("clean_text"),  F.col("lang")))
        .withColumn("extended_positive_lexicon_count", count_extended_positive_cond(F.col("clean_text"), F.col("lang")))
        .withColumn("extended_negative_lexicon_count", count_extended_negative_cond(F.col("clean_text"), F.col("lang")))
        .withColumn("emoji_polarity",          emoji_polarity(F.col("text")))
        .withColumn("punctuation_count_raw",   count_punctuation(F.col("text")))
        .withColumn("punctuation_count",       F.least(F.col("punctuation_count_raw"), F.lit(30)))
        .drop("punctuation_count_raw")
    )

    # VADER (EN only)
    en = (df.filter(F.col("lang")=="en")
            .select("ids","clean_text")
            .withColumn("vader_score", vader_polarity_pd(F.col("clean_text")))
            .withColumn("vader_bucket",
                F.when(F.col("vader_score")<=-0.4, F.lit(0))
                 .when(F.col("vader_score")>= 0.4, F.lit(1))
                 .otherwise(F.lit(2))))
    df = (df.join(en.select("ids","vader_score","vader_bucket"), on="ids", how="left")
            .fillna({"vader_score":0.0, "vader_bucket":2}))

    # word & char TF-IDF
    d1 = tfidf_word_model.transform(df)                               # -> tfidf_l2
    d2 = d1.withColumn("char_ngrams", make_char_ngrams(F.col("clean_text")))
    d3 = tfidf_char_model.transform(d2)                               # -> char_tfidf_l2

    # numeric vector + scale
    missing = [c for c in NUMERIC_ORDER if c not in d3.columns]
    if missing:
        raise RuntimeError(f"Missing numeric columns: {missing}")
    num_assembler = VectorAssembler(inputCols=NUMERIC_ORDER, outputCol="numeric_raw", handleInvalid="keep")
    d4 = num_assembler.transform(d3)
    d5 = scaler_model.transform(d4)                                   # -> numeric_scaled

    # final features (same concat order as training)
    final_assembler = VectorAssembler(
        inputCols=["tfidf_l2","char_tfidf_l2","numeric_scaled"],
        outputCol="features_final",
        handleInvalid="keep"
    )
    out = final_assembler.transform(d5)
    return out

features_df = build_features(scorable)

# ============== Helper: PN BERT ONNX probs with temperature ==============
def bert_pos_probs(texts: List[str], bs: int = 128) -> np.ndarray:
    if not texts: return np.empty((0,), dtype=np.float32)
    outs = []
    for i in range(0, len(texts), bs):
        enc = tokenizer(texts[i:i+bs], return_tensors="np", truncation=True,
                        max_length=MAX_LEN, padding=True)
        logits = onnx_sess.run(None, {"input_ids": enc["input_ids"],
                                      "attention_mask": enc["attention_mask"]})[0]
        logits = logits / max(T_STAR, 1e-3)
        e = np.exp(logits - logits.max(axis=1, keepdims=True))
        p = e / e.sum(axis=1, keepdims=True)
        outs.append(p[:,1])  # positive class
    return np.concatenate(outs, axis=0)

# ============== Minute upsert (you asked for this verbatim) ==============
def upsert_minute_counts(df):
    from pymongo import MongoClient, ReplaceOne
    if not USE_MONGO:
        return
    cli = MongoClient(MONGO_URI_BASE); col = cli[MONGO_DB]["sentiment_minute"]
    rows = (df.select(
                F.date_format("created_at_ts","yyyy-MM-dd HH:mm:00").alias("minute"),
                "sentiment_label"
           ).groupBy("minute","sentiment_label").count().collect())
    ops=[]
    for r in rows:
        s = int(r["sentiment_label"])
        key = {"minute": r["minute"], "sentiment": {1:"positive",2:"neutral",0:"negative"}[s]}
        doc = {"$set": {"minute": r["minute"], "sentiment": key["sentiment"], "cnt": int(r["count"])}}
        ops.append(ReplaceOne(key, doc, upsert=True))
    if ops: col.bulk_write(ops, ordered=False)

# ============== ForeachBatch: do hierarchical scoring, then write ==============
def write_batch(df, epoch_id: int):
    micro = None
    try:
        micro = df.coalesce(1).persist(StorageLevel.MEMORY_AND_DISK)
        rows = micro.count()
        print(f"[foreachBatch] epoch={epoch_id} rows={rows}")
        if rows == 0:
            return

        # Pull columns needed for scoring into pandas / numpy
        pdf = (micro
               .select("ids","text","clean_text","lang","created_at_ts",
                       vector_to_array("features_final").alias("X"))
               .toPandas())

        ids     = pdf["ids"].tolist()
        texts   = pdf["clean_text"].astype(str).tolist()
        langs   = pdf["lang"].astype(str).tolist()
        X       = np.stack(pdf["X"].to_numpy(), axis=0).astype(np.float32)

        # ---- Stage 1: Neutral Gate ----
        p_neu_raw = gate.predict_proba(X)[:,1]  # prob(neutral) before calibration
        p_neu     = iso.transform(p_neu_raw)    # calibrated prob(neutral)
        p_pn      = 1.0 - p_neu

        # Strong neutral & PN floor masks (same logic)
        is_neu_strong = (p_neu >= T_NEU) & (p_neu >= p_pn + NEUTRAL_DELTA)
        pn_ok         = (p_pn >= PN_FLOOR)
        non_mask      = (~is_neu_strong) & pn_ok
        idx_non       = np.where(non_mask)[0]

        # Default outputs (neutral everywhere)
        y_hat = np.full(len(ids), 1, dtype=np.int64)  # 0=neg,1=neu,2=pos
        # default blended P(pos) placeholder (for debug / consistency)
        p_pos_blend_full = np.full(len(ids), 0.5, dtype=np.float32)

        if idx_non.size > 0:
            # ---- Stage 2: PN on routed subset ----
            texts_non = [texts[i] for i in idx_non]
            X_non     = X[idx_non]

            # BERT pos prob (with temperature)
            p_pos_bert = bert_pos_probs(texts_non)

            # XGB PN pos prob
            p_pos_xgb  = xgb_pn.predict_proba(X_non)[:,1]

            # Blend in logit space
            def _logit(p):
                p = np.clip(p, 1e-6, 1-1e-6)
                return np.log(p/(1-p))
            Z = np.column_stack([_logit(p_pos_bert), _logit(p_pos_xgb)])
            p_pos_blend = pn_blend.predict_proba(Z)[:,1]
            p_pos_blend_full[idx_non] = p_pos_blend

            # consensus + confidence
            agree    = ((p_pos_bert >= 0.5) == (p_pos_xgb >= 0.5))
            conf_b   = 0.5 + np.abs(p_pos_bert - 0.5)
            conf_x   = 0.5 + np.abs(p_pos_xgb  - 0.5)
            conf_avg = (conf_b + conf_x) / 2.0
            ok = (agree &
                  (conf_avg >= T_CONF) &
                  (np.abs(p_pos_blend - 0.5) >= MARGIN))

            # PN decisions (only where ok)
            pn_dec = np.where(p_pos_blend >= T_POS, 2, 0)          # 2=pos, 0=neg
            idx_ok = idx_non[ok]
            y_hat[idx_ok] = pn_dec[ok]

        # probs vector (neg, neu, pos) in label_order ["negative","neutral","positive"]
        p_pos = p_pos_blend_full
        p_neg = 1.0 - p_neu - p_pos
        # clip and renorm to be safe
        p_neg = np.maximum(p_neg, 0.0)
        s = (p_neg + p_neu + p_pos).clip(min=1e-6)
        p_neg, p_neu, p_pos = p_neg/s, p_neu/s, p_pos/s

        # sentiment text + numeric code (your dashboard mapping)
        idx2txt = {0:"negative", 1:"neutral", 2:"positive"}
        sent_txt = [idx2txt[i] for i in y_hat]
        sent_num = [1 if t=="positive" else (2 if t=="neutral" else 0) for t in sent_txt]

        # Assemble back to Spark DataFrame
        out_pdf = {
            "ids": ids,
            "text": pdf["text"].tolist(),
            "clean_text": texts,
            "lang": langs,
            "created_at_ts": pdf["created_at_ts"].tolist(),
            "sentiment": sent_txt,
            "sentiment_label": sent_num,
            "p_pos": p_pos.tolist(),
            "p_neu": p_neu.tolist(),
            "probs_arr": [ [float(a), float(b), float(c)] for a,b,c in zip(p_neg, p_pos, p_neu) ]  # keep (neg,pos,neu)
        }
        scored = spark.createDataFrame(
            list(zip(*out_pdf.values())),
            schema=T.StructType([
                T.StructField("ids", T.StringType()),
                T.StructField("text", T.StringType()),
                T.StructField("clean_text", T.StringType()),
                T.StructField("lang", T.StringType()),
                T.StructField("created_at_ts", T.TimestampType()),
                T.StructField("sentiment", T.StringType()),
                T.StructField("sentiment_label", T.IntegerType()),
                T.StructField("p_pos", T.DoubleType()),
                T.StructField("p_neu", T.DoubleType()),
                T.StructField("probs_arr", T.ArrayType(T.DoubleType())),
            ])
        ).withColumn("processed_at", F.current_timestamp())

        # Quick metrics
        dist = (scored.groupBy("sentiment_label").count().orderBy("sentiment_label").collect())
        print("[dbg] label_dist:", [(int(r[0]) if r[0] is not None else None, int(r[1])) for r in dist])

        # ----- Write to Kafka -----
        out_kafka = scored.selectExpr(
            "CAST(ids AS STRING) AS key",
            "to_json(named_struct("
            " 'tweet_id', ids,"
            " 'text', text,"
            " 'clean_text', clean_text,"
            " 'sentiment', sentiment,"
            " 'sentiment_label', sentiment_label,"
            " 'probs', probs_arr,"
            " 'p_neu', p_neu,"
            " 'p_pos', p_pos,"
            " 'created_at', created_at_ts,"
            " 'processed_at', processed_at,"
            " 'category', '',"
            " 'hasLink', null,"
            " 'origin', '',"
            " 'lang', lang,"
            " 'model', 'hier_xgb+bertweet',"
            " 'model_name', '" + MODEL_NAME + "'"
            ") ) AS value"
        )
        (out_kafka.write
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", OUT_TOPIC)
            .save())

        # ----- Mongo (optional) -----
        if USE_MONGO:
            latest = (scored
                .withColumn("_id", F.col("ids"))
                .select(
                    "_id", "ids", F.col("ids").alias("tweet_id"),
                    "text", "clean_text",
                    "sentiment", "sentiment_label", "p_neu", "p_pos", "probs_arr",
                    F.col("created_at_ts").alias("created_at"),
                    F.to_date("created_at_ts").alias("created_day"),
                    F.to_date("processed_at").alias("processed_day"),
                    F.lit("scorer").alias("source"), F.lit("tweet").alias("type"),
                    "lang"
                )
            )
            (latest.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
                .option("spark.mongodb.write.database", MONGO_DB)
                .option("spark.mongodb.write.collection", MONGO_COLL)
                .option("spark.mongodb.write.operationType", "replace")
                .mode("append")
                .save())

            hist = (scored
                .withColumn("_id", F.concat_ws("_", F.col("ids"), F.date_format(F.to_date("created_at_ts"), "yyyyMMdd")))
                .select(
                    "_id", "ids", F.col("ids").alias("tweet_id"),
                    "text", "clean_text",
                    "sentiment", "sentiment_label", "p_neu", "p_pos", "probs_arr",
                    F.col("created_at_ts").alias("created_at"),
                    F.to_date("created_at_ts").alias("created_day"),
                    F.to_date("processed_at").alias("processed_day"),
                    F.lit("scorer").alias("source"), F.lit("tweet").alias("type"),
                    "lang"
                )
            )
            (hist.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
                .option("spark.mongodb.write.database", MONGO_DB)
                .option("spark.mongodb.write.collection", MONGO_COLL_HISTORY)
                .option("spark.mongodb.write.operationType", "replace")
                .mode("append")
                .save())

            metrics = (scored
                .agg(F.count(F.lit(1)).alias("rows"))
                .withColumn("batch_id", F.lit(int(epoch_id)))
                .withColumn("ts", F.current_timestamp()))
            (metrics.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGO_URI_BASE)
                .option("spark.mongodb.write.database", MONGO_DB)
                .option("spark.mongodb.write.collection", MONGO_COLL_METRICS)
                .mode("append")
                .save())

            # minute upsert
            upsert_minute_counts(scored)

    except Exception:
        print("[foreachBatch] ERROR\n" + traceback.format_exc())
    finally:
        if micro is not None:
            micro.unpersist()

# ============== Start stream ==============
spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")

query = (features_df
    .writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "main"))
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start())

print("▶️  Scoring stream started →", OUT_TOPIC)
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    pass