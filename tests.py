from django.test import TestCase
from .models import Tweet

class TweetModelTest(TestCase):
    def test_str(self):
        t = Tweet.objects.create(text="Hello", sentiment="pos")
        self.assertTrue("Positive" in str(t))
