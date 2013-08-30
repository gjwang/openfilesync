# Create your views here.
from django.http import HttpResponse
from celeryfilesync import tasks

def test_celery(request):
    #result = tasks.sleeptask.delay(10)
    #result_one = tasks.sleeptask.delay(10)
    #result_two = tasks.sleeptask.delay(10)
    
    result = tasks.add.delay(4, 4);
    #print result.ready()
    return HttpResponse(result.get())
    #return HttpResponse(result.get())
