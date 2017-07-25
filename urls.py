from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),

    url(r'^(?P<resource_id>.*)/(?P<query_string>.+)/csv$', views.query_csv_view, name='query_csv_view'), 
    url(r'^(?P<resource_id>.*)/(?P<query_string>.+)/$', views.parse_and_query, name='parse_and_query'), 
    url(r'^(?P<resource_id>.*)/(?P<field>.*)/(?P<search_term>.*)/csv$', views.csv_view, name='show_csv'),
    url(r'^(?P<resource_id>.*)/(?P<field>.*)/(?P<search_term>.*)$', views.results, name='results'),
    ]
