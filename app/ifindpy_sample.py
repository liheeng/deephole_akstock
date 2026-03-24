import requests
import json

REFRESH_TOKEN ='eyJzaWduX3RpbWUiOiIyMDI2LTAzLTIzIDExOjM4OjIyIn0=.eyJ1aWQiOiI3NjA2MjMxOTEiLCJ1c2VyIjp7ImFjY291bnQiOiJibHpjZDAwMSIsImF1dGhVc2VySW5mbyI6e30sImNvZGVDU0kiOltdLCJjb2RlWnpBdXRoIjpbXSwiaGFzQUlQcmVkaWN0IjpmYWxzZSwiaGFzQUlUYWxrIjpmYWxzZSwiaGFzQ0lDQyI6ZmFsc2UsImhhc0NTSSI6ZmFsc2UsImhhc0V2ZW50RHJpdmUiOmZhbHNlLCJoYXNGVFNFIjpmYWxzZSwiaGFzRmFzdCI6ZmFsc2UsImhhc0Z1bmRWYWx1YXRpb24iOmZhbHNlLCJoYXNISyI6dHJ1ZSwiaGFzTE1FIjpmYWxzZSwiaGFzTGV2ZWwyIjpmYWxzZSwiaGFzUmVhbENNRSI6ZmFsc2UsImhhc1RyYW5zZmVyIjpmYWxzZSwiaGFzVVMiOmZhbHNlLCJoYXNVU0FJbmRleCI6ZmFsc2UsImhhc1VTREVCVCI6ZmFsc2UsIm1hcmtldEF1dGgiOnsiRENFIjpmYWxzZX0sIm1heE9uTGluZSI6MSwibm9EaXNrIjpmYWxzZSwicHJvZHVjdFR5cGUiOiJTVVBFUkNPTU1BTkRQUk9EVUNUIiwicmVmcmVzaFRva2VuRXhwaXJlZFRpbWUiOiIyMDI2LTA0LTAzIDExOjE5OjI3Iiwic2Vzc3Npb24iOiIyYTE0OWE1OTFkNzZiZjA3MDc0MmFhMGViOWJkN2Y0MiIsInNpZEluZm8iOns2NDoiMTExMTExMTExMTExMTExMTExMTExMTExIiwxOiIxMDEiLDI6IjEiLDY3OiIxMDExMTExMTExMTExMTExMTExMTExMTEiLDM6IjEiLDY5OiIxMTExMTExMTExMTExMTExMTExMTExMTExIiw1OiIxIiw2OiIxIiw3MToiMTExMTExMTExMTExMTExMTExMTExMTAwIiw3OiIxMTExMTExMTExMSIsODoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEiLDEzODoiMTExMTExMTExMTExMTExMTExMTExMTExMSIsMTM5OiIxMTExMTExMTExMTExMTExMTExMTExMTExIiwxNDA6IjExMTExMTExMTExMTExMTExMTExMTExMTEiLDE0MToiMTExMTExMTExMTExMTExMTExMTExMTExMSIsMTQyOiIxMTExMTExMTExMTExMTExMTExMTExMTExIiwxNDM6IjExIiw4MDoiMTExMTExMTExMTExMTExMTExMTExMTExIiw4MToiMTExMTExMTExMTExMTExMTExMTExMTExIiw4MjoiMTExMTExMTExMTExMTExMTExMTEwMTEwIiw4MzoiMTExMTExMTExMTExMTExMTExMDAwMDAwIiw4NToiMDExMTExMTExMTExMTExMTExMTExMTExIiw4NzoiMTExMTExMTEwMDExMTExMDExMTExMTExIiw4OToiMTExMTExMTEwMTEwMTAwMDAwMDAxMTExIiw5MDoiMTExMTEwMTExMTExMTExMTEwMDAxMTExMTAiLDkzOiIxMTExMTExMTExMTExMTExMTAwMDAxMTExIiw5NDoiMTExMTExMTExMTExMTExMTExMTExMTExMSIsOTY6IjExMTExMTExMTExMTExMTExMTExMTExMTEiLDk5OiIxMDAiLDEwMDoiMTExMTAxMTExMTExMTExMTExMCIsMTAyOiIxIiw0NDoiMTEiLDEwOToiMSIsNTM6IjExMTExMTExMTExMTExMTExMTExMTExMSIsNTQ6IjExMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiw1NzoiMDAwMDAwMDAwMDAwMDAwMDAwMDAxMDAwMDAwMDAiLDYyOiIxMTExMTExMTExMTExMTExMTExMTExMTEiLDYzOiIxMTExMTExMTExMTExMTExMTExMTExMTEifSwidGltZXN0YW1wIjoiMTc3NDIzNzEwMjQ5MCIsInRyYW5zQXV0aCI6ZmFsc2UsInR0bFZhbHVlIjowLCJ1aWQiOiI3NjA2MjMxOTEiLCJ1c2VyVHlwZSI6IkZSRUVJQUwiLCJ3aWZpbmRMaW1pdE1hcCI6e319fQ==.D1F0D33C1379DEDD30CE8078849A9C1AE5E6CA808EC337C79B66E2F96BF5DBEE'
    
def get_access_token(refresh_token: str):
    getAccessTokenUrl = 'https://quantapi.51ifind.com/api/v1/get_access_token'
    getAccessTokenHeader = {"Content-Type":"application/json", "refresh_token": refresh_token}
    getAccessTokenResponse=requests.post(url=getAccessTokenUrl,headers=getAccessTokenHeader)
    accessToken = json.loads(getAccessTokenResponse.content)['data']['access_token'] 
    print(accessToken)
    return accessToken

def get_realtime_quotation(code: str, accessToken: str):
    thsUrl = 'https://quantapi.51ifind.com/api/v1/realtime__quotation'
    thsHeaders = {"Content-Type":"application/json", "access_token":accessToken}
    thsPara = {"codes":code , "indicators":"open,high,low,latest"}
    thsResponse = requests.post(url=thsUrl, json=thsPara, headers=thsHeaders)
    print(thsResponse.content)

def get_historical_data(codes: str, accessToken: str):
    thsUrl = 'https://quantapi.51ifind.com/api/v1/cmd_history_quotation'
    thsHeaders = {"Content-Type":"application/json", "access_token":accessToken}
    thsPara ={
        "codes": codes,
        "indicators": "open,close,volume",
        "startdate": "2024-08-25",
        "enddate": "2025-08-25",
        "functionpara": {
            "Interval": "W",
            "CPS": "3",
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