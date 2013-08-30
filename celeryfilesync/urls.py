'''
Created on 2013-7-29

@author: Administrator
'''

from django.conf.urls import patterns, url

from celeryfilesync import views

urlpatterns = patterns('',
    # ex: /polls/
    url(r'^$', views.test_celery, name='test_celery'),
    # the 'name' value as called by the {% url %} template tag
    # ex: /polls/5/
    #url(r'^(?P<poll_id>\d+)/$', views.detail, name='detail'),
    # ex: /polls/5/results/
    #url(r'^(?P<poll_id>\d+)/results/$', views.results, name='results'),
    # ex: /polls/5/vote/
    #url(r'^(?P<poll_id>\d+)/vote/$', views.vote, name='vote'),
)
