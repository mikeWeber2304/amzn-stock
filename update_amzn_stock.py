import mws
import json
import requests
from time import sleep
import re
import datetime
import configparser

#getting config
config = configparser.ConfigParser()
config.read('../account.ini')
conf = config['MANNE']

#initialising
access_key = conf['aws_access_key_id']
secret_key = conf['secret_key']
account_id = conf['seller_id']
auth_token = conf['mws_auth_token']
bigbuy_api_key = conf['bigbuy_api_key']
de_marketplace = 'A1PA6795UKMFR9'
region = 'DE'

#initialising
bigbuy_url = 'https://api.bigbuy.eu'
bb_header = {'Authorization': 'Bearer ' + bigbuy_api_key}
feed_api = mws.Feeds(access_key=access_key, secret_key=secret_key, account_id=account_id, auth_token=auth_token)
products_api = mws.Products(access_key=access_key, secret_key=secret_key, account_id=account_id, auth_token=auth_token)
reports_api = mws.Reports(access_key=access_key, secret_key=secret_key, account_id=account_id, auth_token=auth_token,
                                       region=region)

def get_sku_list():
#changed report_type
    report_request = reports_api.request_report(report_type='_GET_MERCHANT_LISTINGS_ALL_DATA_', marketplaceids=de_marketplace)
    if report_request.response.status_code == 200:
        starting_date = datetime.datetime.now() - datetime.timedelta(hours=6)
        sleep(60)
        report_list = reports_api.get_report_list(fromdate=starting_date)
        newest_report_meta_list = list(
            filter(lambda x: x.ReportType == '_GET_MERCHANT_LISTINGS_ALL_DATA_', report_list.parsed.ReportInfo))
        while bool(report_list.parsed['HasNext']['value']) and len(newest_report_meta_list) < 1:
            report_list = reports_api.get_report_list(fromdate=starting_date,
                                                           next_token=report_list.parsed['NextToken']['value'])
            newest_report_meta_list = list(
                filter(lambda x: x.ReportType == '__GET_MERCHANT_LISTINGS_ALL_DATA_', report_list.parsed.ReportInfo))
        report = reports_api.get_report(newest_report_meta_list[0].ReportId)
        parsed_report = report.parsed.decode('iso-8859-1').split('\n')
        amazon_listing = [x for x in list(map(lambda x: x.split('\t'), parsed_report)) if len(x) > 3]
        sku_list = list(map(lambda x: x[3] if not len(x) < 3 else 0, amazon_listing))
    return sku_list

def get_stock_of_all_products_in_list(amazon_skus):
#gets all stock with bb, create dict, calc waiting time
    #pattern check
    pattern = re.compile('^([A-Z]{1}[0-9]{7})$')
    amazon_skus_new = []
    for sku in amazon_skus:
        if pattern.match(sku):
            amazon_skus_new.append(sku)
    sku_dict = create_sku_dict_for_list(amazon_skus_new)
    stock_object = {"product_stock_request": {"products": sku_dict}}
    json_body = json.dumps(stock_object)
    r0 = requests.post(bigbuy_url + '/rest/catalog/productsstockbyreference.json', data=json_body, headers=bb_header)
    if r0.status_code == 200:
        answer = json.loads(r0.text)
        keys = []
        values = []
        for index in range(answer.__len__()):
            keys.append(answer[index]['sku'])
            currentStock = answer[index]['stocks'][0]['quantity'] - 2 #constant could be lowered
            if currentStock < 0:
                currentStock = 0
            values.append(currentStock)
        final_stock_dict = dict(zip(keys, values))
        waiting_time = lambda x: int(round(int(len(x)) * 0.07)) #constant could be lowered, 0.05 worked once, suddenly it was too low
        waiting_time_for_dict = waiting_time(final_stock_dict)
    return final_stock_dict, waiting_time_for_dict

def create_sku_dict_for_list(list):
#this creates dict for json body
    sku_dicts = []
    for item in list:
        keys = []
        values = []
        keys.append("sku")
        values.append(item)
        new_dict = dict(zip(keys, values))
        sku_dicts.append(new_dict)
    return sku_dicts

def generate_xml_header():
    header = '<?xml version="1.0" encoding="UTF-8"?><AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd"><Header><DocumentVersion>1.01</DocumentVersion><MerchantIdentifier>' + str(account_id) + '</MerchantIdentifier></Header><MessageType>Inventory</MessageType>'
    return header

def generate_xml_message(message_id, sku, stock):
    message = '<Message><MessageID>' + str(message_id) + '</MessageID><OperationType>Update</OperationType><Inventory><SKU>' + str(sku) + '</SKU><Quantity>' + str(stock) + '</Quantity><FulfillmentLatency>3</FulfillmentLatency></Inventory></Message>'
    return message

def generate_xml_footer():
    footer = '</AmazonEnvelope>'
    return footer

def generate_xml_body_for_dictionary(dict):
#genereate xml body
    message_id = 0
    body = ''
    for index in range(len(list(dict.values()))):
        message_id += 1
        body += generate_xml_message(message_id=str(message_id), sku=str(list(dict.keys())[index]), stock=str(list(dict.values())[index]))
    return body

def generate_xml_for_dict(dict):
#forge xml
    xml = generate_xml_header() + generate_xml_body_for_dictionary(dict) + generate_xml_footer()
    xml = xml.encode('utf-8')
    return xml

def submit_inventory_feed(xml, waiting_time):
#submitting feed and creating response dict
    feed_submitted = feed_api.submit_feed(feed=xml, feed_type='_POST_INVENTORY_AVAILABILITY_DATA_', marketplaceids=de_marketplace)
    if feed_submitted.response.status_code == 200:
        feed_submission_id = feed_submitted._response_dict['SubmitFeedResult']['FeedSubmissionInfo']['FeedSubmissionId']['value']
        feed_date_0 = feed_submitted._response_dict['SubmitFeedResult']['FeedSubmissionInfo']['SubmittedDate']['value']
        feed_date_1 = feed_date_0.split('-')
        feed_date = feed_date_1[2][0:2] + '.' + feed_date_1[1] + '.' + feed_date_1[0] + ' ' + feed_date_1[2][3:11] + ' (UTC TIME)'
        feed_status = feed_submitted._response_dict['SubmitFeedResult']['FeedSubmissionInfo']['FeedProcessingStatus']['value']
        status_code_submission = feed_submitted.response.status_code
        sleep(waiting_time)
        feed_result = feed_api.get_feed_submission_result(feedid=feed_submission_id)
        feed_result_status = feed_result._response_dict['Message']['ProcessingReport']['StatusCode']['value']
        messages_total = feed_result._response_dict['Message']['ProcessingReport']['ProcessingSummary']['MessagesProcessed']['value']
        messages_successful = feed_result._response_dict['Message']['ProcessingReport']['ProcessingSummary']['MessagesSuccessful']['value']
        messages_error = feed_result._response_dict['Message']['ProcessingReport']['ProcessingSummary']['MessagesWithError']['value']
        messages_warning = feed_result._response_dict['Message']['ProcessingReport']['ProcessingSummary']['MessagesWithWarning']['value']
        status_code_response = feed_result.response.status_code
        keys = ['SubmissionID', 'Datum', 'SubmissionStatus', 'SubmissionStatusCode', 'ResultStatus', 'ÄnderungenGesamt', 'ÄnderungenErfolgreich', 'ÄnderungenVerfehlt', 'ÄnderungenWarnung', 'ResultStatusCode']
        values = [feed_submission_id, feed_date, feed_status, status_code_submission, feed_result_status, messages_total, messages_successful, messages_error, messages_warning, status_code_response]
        feed_response = dict(zip(keys, values))
    return feed_response

def update_stock():
#main function
    sku_list = get_sku_list()
    print('GOT SKU LIST, LENGTH: ' + str(len(sku_list)))
    all_skus_and_stock, waiting_time = get_stock_of_all_products_in_list(sku_list)
    print('DICT CREATED, LENGTH: ' + str(len(all_skus_and_stock)))
    xml = generate_xml_for_dict(all_skus_and_stock)
    print('XML CREATED, REQUEST STARTING, WAITING TIME: ' + str(waiting_time) + ' SECONDS')
    feed_response = submit_inventory_feed(xml=xml, waiting_time=waiting_time)
    print(feed_response)
    print('STOCKS UPDATED, SEE MORE INFO ABOVE')

update_stock()