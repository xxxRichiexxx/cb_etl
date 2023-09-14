import requests
import pandas as pd




data = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <NewsInfoXML xmlns="http://web.cbr.ru/">
                <fromDate>2023-09-01</fromDate>
                <ToDate>2023-09-30</ToDate>
                </NewsInfoXML>
            </soap:Body>
            </soap:Envelope>"""

# data = """<?xml version="1.0" encoding="utf-8"?>
# <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
#   <soap:Body>
#     <AllDataInfoXML xmlns="http://web.cbr.ru/" />
#   </soap:Body>
# </soap:Envelope>
headers = {'content-type': 'text/xml'}

response = requests.post(
    'https://cbr.ru/DailyInfoWebServ/DailyInfo.asmx',
    data = data,
    headers = headers,
    verify=False,
)
response.raise_for_status()

print(response.text)

data = pd.read_xml(response.text,
                   xpath="//News"
                   )

print(data)



