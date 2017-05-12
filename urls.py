from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^(?P<resource_id>.*)/(?P<field>.*)/(?P<search_term>.*)/csv$', views.csv_view, name='show_csv'),
    url(r'^(?P<resource_id>.*)/(?P<field>.*)/(?P<search_term>.*)$', views.results, name='results'),
    ]
