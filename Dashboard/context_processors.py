from django import get_version

def django_version(request):
    return {"django_version": get_version()}
