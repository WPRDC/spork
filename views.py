from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
import requests
from json2html import *
import ckanapi
from pprint import pprint
from datetime import datetime

import re, csv, sys
try:
    sys.path.insert(0, '/Users/drw/WPRDC') # A path that we need to import code from
    from utility_belt.gadgets import get_schema, schema_dict, get_resource_parameter, get_package_name_from_resource_id
except:
    sys.path.insert(0, '/Users/daw165/bin/')# Office computer location
    from utility_belt.gadgets import get_schema, schema_dict, get_resource_parameter, get_package_name_from_resource_id

# [ ] All CKAN API requests should be API-key-free to avoid any possibility of tables 
# being dropped or data being modified.

# [ ] Implement a smart limiter which estimates the memory footprints required by running this script
# and processing the query on the CKAN instance and adjusts as necessary.

# Front-end stuff
# [ ] Implement GUI (probably using JQuery Query Builder, though also consider django-report-builder)
# [ ] Add checkbox to drop duplicate rows and implement by changing SELECT to SELECT DISTINCT.

DEFAULT_SITE = "https://data.wprdc.org"

def convert_booleans_to_text(rows):
    for r in rows:
        for key in r:
            if type(r[key]) == bool:
                r[key] = "{}".format(r[key])
    return rows

def eliminate_field(schema,field_to_omit):
    new_schema = []
    for s in schema:
        if s['id'] != field_to_omit:
            new_schema.append(s)
    return new_schema

def total_rows(ckan,query):
    #row_counting_query = re.sub('^SELECT .* FROM', 'SELECT COUNT(*) as "row_count" FROM', query)
    row_counting_query = 'SELECT COUNT(*) FROM ({}) subresult'.format(query)
    print("row_counting_query = {}".format(row_counting_query))
    r = ckan.action.datastore_search_sql(sql=row_counting_query)
    #for k in r.keys():
    #    if k == 'records':
    #        print("'records':")
    #        pprint(r[k][0:10])
    #    else:
    #        print("'{}': {}".format(k,r[k]))
    count = int(r['records'][0]['count'])
    return count

def get_and_write_next_rows(ckan,resource_id,query,field,search_term,writer,chunk_size,offset=0,written=0):
    if query is None:
        r = ckan.action.datastore_search(id=resource_id, limit=chunk_size, offset=offset, filters={field: search_term}) 
    else:
        query += " LIMIT {} OFFSET {}".format(chunk_size,offset)
        r = ckan.action.datastore_search_sql(sql=query)
    data = r['records']
    schema = eliminate_field(r['fields'],'_full_text')
    # Exclude _full_text from the schema.

    ordered_fields = [f['id'] for f in schema]

    if written == 0:
        writer.writerow(ordered_fields)
  
    for row in data:
        writer.writerow([row[f] for f in ordered_fields]) 

    if 'total' in r:
        total = r['total']
    else:
        total = total_rows(ckan,query)

    return written+len(data), total

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

    # [ ] Eventually merge these changes with utility_belt/gadgets.py
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
    data_table = json2html.convert(convert_booleans_to_text(data))
    # This should be fixed eventually:
    #       https://github.com/softvar/json2html/pull/9
    if 'records' in r:
        r['records'] = convert_booleans_to_text(r['records'])
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

def convert_operator(op):
    if op == 'eq' or op == '':
        return '='
    if op in ['!','!=','<>']:
        return '!='
    if op in ['!~']:
        return '!~'
    if op == '~':
        return 'LIKE'
    if op == 'lt':
        return '<'
    if op == 'gt':
        return '>'
    raise ValueError("{} is an operator for which there is not yet a conversion".format(op))

def generate_query(resource_id,schema,query_string=''):
    filter_strings = []
    groupbys = []
    aggregators = []
    orderbys = []
    if query_string != '':
        elements = query_string.split('/')
        op = None
        for e in elements:
            q_list = e.split('--')
            # Possible formats: 
            #    /field1--value1/field2--value2/ (where equality is implicit)
            #   /field1--eq--value1/field2--lt--value2/groupby--field3/aggregateby--sum--field4/
            #       (equality (and other relations) are explicit)
            if q_list == ['']:
                pass
            elif len(q_list) == 3: # It's a filter
                if q_list[0] == 'aggregateby':
                    agg = q_list[1].upper()
                    if agg in ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT']:
                        aggregators.append('{}("{}")'.format(q_list[1],q_list[2])) 
                    else:
                        raise ValueError('Unknown aggregator function {} found'.format(q_list[1]))
                    # In the URL, aggregators are of the form
                    # /aggregateby--sum--field_name/
                    # and then they become functions in the 
                    # aggregators list, such that aggregators looks like
                    #   aggregators = ['SUM(field_name)','AVG(whoa)']

                    # Since parentheses are allowed, this could be changed to 
                    # /sum(field_name)/
                    # The available symbols, which don't get URL-encoded, are $-_.+!*'(),
                    # Thus, even my squiggle is a little potentially problematic.

                elif q_list[0] == 'orderby':
                    field = q_list[1]
                    direction = q_list[2].upper()
                    if direction in ['ASC','DESC','']:
                        orderbys.append('"{}" {}'.format(field,direction)) 
                    else:
                        raise ValueError('Unknown ordering direction {} found'.format(q_list[2]))
                else: 
                    #r = query_resource(site,  'SELECT * FROM "{}" WHERE venue_type = \'Church\' LIMIT 3'.format(resource_id), API_key)
                    # Knowing the types of the fields is important for formatting the query
                    schema_types = schema_dict(schema)
                    field_type = schema_types[q_list[0]]
                    if field_type in ['numeric','float8','int4','int8']:
                        filter_s = '"{}" {} {}'.format(q_list[0], convert_operator(q_list[1]), q_list[2])
                    elif field_type in ['text','JSON','json']: 
                        op = convert_operator(q_list[1])
                        filter_s = '"{}" {} '.format(q_list[0], op)
                        if op == 'LIKE':
                            filter_s += "'%{}%'".format(q_list[2])
                        else:
                            filter_s += "'{}'".format(q_list[2])
                    elif field_type in ['bool','boolean']: 
                        op = convert_operator(q_list[1])
                        if op in ['!~','!=']:
                            filter_s = '("{}" != {} OR "{}" IS NULL)'.format(q_list[0],q_list[2],q_list[0])
                            # This is how boolean fields work in Postgres:
                            #    SELECT * FROM test1;
                            #     a |    b
                            #    ---+---------
                            #     t | sic est
                            #     f | non est

                            #    SELECT * FROM test1 WHERE a;
                            #     a |    b
                            #    ---+---------
                            #     t | sic est
                            # https://www.postgresql.org/docs/9.1/static/datatype-boolean.html

                            # CREATE TABLE test1 (a boolean, b text);
                            # INSERT INTO test1 VALUES (TRUE, 'sic est');
                            # INSERT INTO test1 VALUES (FALSE, 'non est');
                            # INSERT INTO test1 VALUES (NULL, 'nemo');
                            # SELECT * FROM test1 WHERE NOT a;
                            # |     a |       b |
                            # |-------|---------|
                            # | false | non est |

                            # SELECT * FROM test1 WHERE a IS NULL; (Null is not a value and 
                            # can only be detected this way).
                            #        |      a |    b |
                            #        |--------|------|
                            #        | (null) | nemo |
                            
                            # So two operators are desirable: x = False and x != True (where x != True
                            # means x = True OR x IS NULL.

                            # Maybe we can get away without implementing ORs by noting that we can
                            # always AND together negated filters and then negate the output?
                            # But then we need a way of negating the overall filter.
                        else:
                            filter_s = '"{}" {} {}'.format(q_list[0], convert_operator(q_list[1]), q_list[2])
                    elif field_type in ['date','timestamp']: 
                        # /start_date--gt--2016-03-01/
                        # /start_date--gt--2016-03-01-1300/
                        op = convert_operator(q_list[1])
                        try:
                            date_limit = q_list[2]
                            limiting_date = datetime.strptime(date_limit, "%Y-%m-%d")
                            filter_s = '"{}" {} '.format(q_list[0], op)
                            filter_s += "'{}'".format(q_list[2])
                        except:
                            datetime_limit = q_list[2]
                            limiting_dt = datetime.strptime(datetime_limit, "%Y-%m-%d-%H%M")
                            filter_s = '"{}" {} '.format(q_list[0], op)
                            limiting_dt_s = datetime.strftime(limiting_dt, '%Y-%m-%d %H:%M:%S')
                            filter_s += "'{}'".format(limiting_dt_s)
                    else:
                        raise ValueError("Modify generate_query to handle fields of type {}".format(field_type))
                    # timestamp, text, bool or boolean | [ ] Some of these others need better handling and/or conversion.
                    # I've also seen 'tsvector' (which is used for the _full_text field returned by SQL query requests
                    # and 'nested' though I don't know if the latter is official.

                    filter_strings.append(filter_s)
            elif len(q_list) == 2:
                if q_list[0] == 'groupby':
                    groupbys.append('"{}"'.format(q_list[1]))
                    # An example of a SQL query with grouping and aggregating:
                    #   SELECT Shippers.ShipperName,COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders
                    #        LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID
                    #        GROUP BY ShipperName;

                    # The result is two columns: ShipperName and NumberOfOrders.

                    # Aggregation goes hand-in-hand with grouping.
                    # Good default aggregations might be number_of_rows and summation of any numeric field.
                elif q_list[0] == 'orderby':
                    field = q_list[1]
                    orderbys.append('"{}" ASC'.format(field,direction)) 
                else:
                    raise ValueError("Unable to process the element {}".format(q_list))
            else: 
                raise ValueError("q_list is {} elements long, which is an unexpected length".format(len(q_list)))

    query = 'SELECT * FROM "{}"'.format(resource_id)
    if len(groupbys) > 0:
        query = 'SELECT '
        if len(groupbys) > 0:
         query += '{}, '.format(', '.join(groupbys))
        if len(aggregators) > 0:
            for a in aggregators:
                query += '{}, '.format(a)
        query += 'COUNT("_id") as "count" FROM "{}"'.format(resource_id)   
    
    if len(filter_strings) > 0:
        query += ' WHERE {}'.format(' AND '.join(filter_strings))
    if len(groupbys) > 0:
        query += ' GROUP BY {}'.format(', '.join(groupbys))
    if len(orderbys) > 0:
        query += ' ORDER BY {}'.format(', '.join(orderbys))

    return query, filter_strings, groupbys, aggregators

def query_csv_view(request,resource_id,query_string):
    # Create the HttpResponse object with the appropriate CSV header.
    site = DEFAULT_SITE
    schema = get_schema(site,resource_id,API_key=None)
    query, filter_strings, groupbys, aggregators = generate_query(resource_id,schema,query_string) 

    name = get_resource_name(site,resource_id)
    name = re.sub(' ','_',name)
    if len(filter_strings) > 0:
        name += "-filtered"
    if len(groupbys) > 0:
        name += "-grouped"

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="{}.csv"'.format(name)

    writer = csv.writer(response)

    offset = 0
    chunk_size = 3000
    ckan = ckanapi.RemoteCKAN(site)
    written, total = get_and_write_next_rows(ckan,resource_id,query,None,None,writer,chunk_size,offset=0,written=0)

    while written < total:
        offset = offset+chunk_size
        written, total = get_and_write_next_rows(ckan,resource_id,query,None,None,writer,chunk_size,offset,written)

    return response

def parse_and_query(request,resource_id,query_string):
    site = DEFAULT_SITE
    schema = get_schema(site,resource_id,API_key=None)
    
    query, filter_strings, groupbys, aggregators = generate_query(resource_id,schema,query_string) 

    ckan = ckanapi.RemoteCKAN(site)
    print("query = {}".format(query))
    r = ckan.action.datastore_search_sql(sql=query + " LIMIT 1000")

    data = r['records']
    for row in data:
        if '_full_text' in row:
            del row['_full_text']

    data_table = json2html.convert(convert_booleans_to_text(data))
    # This should be fixed eventually:
    #       https://github.com/softvar/json2html/pull/9
    if 'records' in r:
        r['records'] = convert_booleans_to_text(r['records'])
    html_table = json2html.convert(r)

    if 'total' in r:
        total = r['total']
    else:
        total = total_rows(ckan,query)

    name = get_resource_name(site,resource_id)
    link = "/spork/{}/{}/csv".format(resource_id,query_string)
    page = """<span><big>Download <a href="https://www.wprdc.org">WPRDC</a> data by the sporkful</big></span><br><br>
        This page shows the first 1000 rows of the resource 
        ({}) that 
        satisfy the filters <i>{}</i>,
        grouped by <b>{}</b>.

        <br><br>
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
        {}
        
        By the way, here is the SQL query that was used to get this data: <br><br>
        <code>{}</code>""".format(name, ', '.join(filter_strings), ', '.join(groupbys), total, link, data_table, html_table, query)
  
    return HttpResponse(page)


def index(request):
    page = """<span><big>Download data by the sporkful</big></span><br><br>
        This page finds the first 1000 rows of a given 
        <a href="http://www.wprdc.org">WPRDC</a> resource
        that contain a given search term.<br><br>
        <br> 
        URL format: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;/spork/[resource id]/[column name]----[search term]/[another column name]----[another search term]/groupby--[column to group by]

        <br><br>
        For instance, searching the tax liens data for block_lot values of 167K98
        can be done with this URL:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;/spork/8cd32648-757c-4637-9076-85e144997ca8/block_lot----167K98
        <br><br>
        (That is, enter the resource you want after the slash in the URL 
        above and then enter another slash and define a filter by specifying the
        field name and the search term, separated by four dashes.)
        <br><br> 
        Here's a more complex query: Search the tax liens data for parcels in Swissvale
        but only County tax liens.
        can be done with this URL:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;/spork/8cd32648-757c-4637-9076-85e144997ca8/municipality----Swissvale Boro/Lien Description----Allegheny County Tax Lien
        <br><br>
        
        """
    return HttpResponse(page)
