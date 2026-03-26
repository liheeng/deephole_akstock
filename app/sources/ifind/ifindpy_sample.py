import requests
import json

def get_access_token(refresh_token: str):
    getAccessTokenUrl = 'https://quantapi.51ifind.com/api/v1/get_access_token'
    getAccessTokenHeader = {"Content-Type":"application/json", "refresh_token": refresh_token}
    getAccessTokenResponse=requests.post(url=getAccessTokenUrl,headers=getAccessTokenHeader)
    accessToken = json.loads(getAccessTokenResponse.content)['data']['access_token'] 
    print(accessToken)
    return accessToken

def get_realtime_quotation(codes: str, accessToken: str):
    thsUrl = 'https://quantapi.51ifind.com/api/v1/realtime__quotation'
    thsHeaders = {"Content-Type":"application/json", "access_token":accessToken}
    thsPara = {"codes": codes , "indicators":"open,high,low,latest"}
    thsResponse = requests.post(url=thsUrl, json=thsPara, headers=thsHeaders)
    print(thsResponse.content)

def get_historical_data(codes: str, accessToken: str):
    thsUrl = 'https://quantapi.51ifind.com/api/v1/cmd_history_quotation'
    thsHeaders = {"Content-Type":"application/json", "access_token":accessToken}
    thsPara ={
        "codes": codes,
        "indicators": "open,close,volume",
        "startdate": "2026-01-01",
        "enddate": "2026-03-25",
        "functionpara": {
            "Interval": "D",
            "CPS": "2",
            "Currency": "RMB",
            "Fill": "Blank",
        }
    }
    thsResponse = requests.post(url=thsUrl, json=thsPara, headers=thsHeaders)
    print(thsResponse.content)

if __name__ == '__main__':
    accessToken = get_access_token(REFRESH_TOKEN)
    # get_realtime_quotation("000001.SZ", accessToken)
    get_historical_data("300033.SZ, 600030.SH", accessToken) # "300033.SZ, 600030.SH"