from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
import requests
from json2html import *
import ckanapi

import csv

DEFAULT_SITE = "https://data.wprdc.org"


def get_and_write_next_rows(ckan,resource_id,field,search_term,writer,chunk_size,offset=0,written=0):
    r = ckan.action.datastore_search(id=resource_id, limit=chunk_size, offset=offset, filters={field: search_term}) 
    data = r['records']
    schema = r['fields']
    ordered_fields = [f['id'] for f in schema]

    if written == 0:
        writer.writerow(ordered_fields)
  
    for row in data:
        writer.writerow([row[f] for f in ordered_fields]) 

    return written+len(data), r['total']

def csv_view(request,resource_id,field,search_term):
    # Create the HttpResponse object with the appropriate CSV header.
    site = DEFAULT_SITE
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="{}.csv"'.format(search_term)

    writer = csv.writer(response)

    offset = 0
    chunk_size = 30000
    ckan = ckanapi.RemoteCKAN(site)
    written, total = get_and_write_next_rows(ckan,resource_id,field,search_term,writer,chunk_size,offset=0,written=0)

    while written < total:
        offset = offset+chunk_size
        written, total = get_and_write_next_rows(ckan,resource_id,field,search_term,writer,chunk_size,offset,written)

    return response

def dealias(site,pseudonym):
    ckan = ckanapi.RemoteCKAN(site)
    aliases = ckan.action.datastore_search(id='_table_metadata',filters={'name': pseudonym})
    resource_id = aliases['records'][0]['alias_of']
    return resource_id

def get_resource_name(site,resource_id):
    # Code borrowed from utility-belt, then mutated.
    try:
        ckan = ckanapi.RemoteCKAN(site)
        metadata = ckan.action.resource_show(id=resource_id)
        desired_string = metadata['name']
    except ckanapi.errors.NotFound:
        # Maybe the resource_id is an alias for the real one.
        real_id = dealias(site,resource_id)

        ckan = ckanapi.RemoteCKAN(site)
        metadata = ckan.action.resource_show(id=real_id)
        desired_string = metadata['name']
    except:
        desired_string = None

    return desired_string

def results(request,resource_id,field,search_term):
    site = DEFAULT_SITE
    ckan = ckanapi.RemoteCKAN(site)
    r = ckan.action.datastore_search(id=resource_id, limit=1000, filters={field: search_term}) #, offset=offset)

    data = r['records']
    data_table = json2html.convert(data)
    html_table = json2html.convert(r)

    if 'total' in r:
        total = r['total']
    else:
        total = 0

    name = get_resource_name(site,resource_id)
    link = "/spork/{}/{}/{}/csv".format(resource_id,field,search_term)
    page = """<span><big>Download <a href="https://www.wprdc.org">WPRDC</a> data by the sporkful</big></span><br><br>
        This page shows the first 1000 rows of the resource 
        ({}) that 
        contain a <i>{}</i> value equal to <b>{}</b>.<br><br>
        Here is a link to a CSV file that holds all {} of the rows:
        <a href="{}">CSV file</a>
        <br>
        <br>
        <br>
        Data preview:
        {}
        <br>
        <br>
        <br><br>Here is a really verbose version of the data: 
        {}""".format(name, field, search_term, total, link, data_table, html_table)
  
    return HttpResponse(page)


def index(request):
    page = """<span><big>Download data by the sporkful</big></span><br><br>
        This page finds the first 1000 rows of a given 
        <a href="http://www.wprdc.org">WPRDC</a> resource
        that contain a given search term.<br><br>
        <br> 
        URL format: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;/spork/[resource id]/[column name]/[search term]
        <br><br>
        For instance, searching Tax Liens data for block_lot values of 167K98
        can be done with this URL:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;/spork/8cd32648-757c-4637-9076-85e144997ca8/block_lot/167K98
        <br><br>
        (That is, enter the resource you want after the slash in the URL 
        above and then enter another slash and the term you want to 
        search for.)"""
    return HttpResponse(page)
