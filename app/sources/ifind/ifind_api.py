import enum
import requests
import json
from datetime import datetime
from sources.data_source import QueryOptions
from sources.ifind.data_adapter import convert_to_df
from typing import Dict
from utils.log_manager import get_logger

logger = get_logger(__name__)


REFRESH_TOKEN = 'eyJzaWduX3RpbWUiOiIyMDI2LTAzLTIzIDExOjM4OjIyIn0=.eyJ1aWQiOiI3NjA2MjMxOTEiLCJ1c2VyIjp7ImFjY291bnQiOiJibHpjZDAwMSIsImF1dGhVc2VySW5mbyI6e30sImNvZGVDU0kiOltdLCJjb2RlWnpBdXRoIjpbXSwiaGFzQUlQcmVkaWN0IjpmYWxzZSwiaGFzQUlUYWxrIjpmYWxzZSwiaGFzQ0lDQyI6ZmFsc2UsImhhc0NTSSI6ZmFsc2UsImhhc0V2ZW50RHJpdmUiOmZhbHNlLCJoYXNGVFNFIjpmYWxzZSwiaGFzRmFzdCI6ZmFsc2UsImhhc0Z1bmRWYWx1YXRpb24iOmZhbHNlLCJoYXNISyI6dHJ1ZSwiaGFzTE1FIjpmYWxzZSwiaGFzTGV2ZWwyIjpmYWxzZSwiaGFzUmVhbENNRSI6ZmFsc2UsImhhc1RyYW5zZmVyIjpmYWxzZSwiaGFzVVMiOmZhbHNlLCJoYXNVU0FJbmRleCI6ZmFsc2UsImhhc1VTREVCVCI6ZmFsc2UsIm1hcmtldEF1dGgiOnsiRENFIjpmYWxzZX0sIm1heE9uTGluZSI6MSwibm9EaXNrIjpmYWxzZSwicHJvZHVjdFR5cGUiOiJTVVBFUkNPTU1BTkRQUk9EVUNUIiwicmVmcmVzaFRva2VuRXhwaXJlZFRpbWUiOiIyMDI2LTA0LTAzIDExOjE5OjI3Iiwic2Vzc3Npb24iOiIyYTE0OWE1OTFkNzZiZjA3MDc0MmFhMGViOWJkN2Y0MiIsInNpZEluZm8iOns2NDoiMTExMTExMTExMTExMTExMTExMTExMTExIiwxOiIxMDEiLDI6IjEiLDY3OiIxMDExMTExMTExMTExMTExMTExMTExMTEiLDM6IjEiLDY5OiIxMTExMTExMTExMTExMTExMTExMTExMTExIiw1OiIxIiw2OiIxIiw3MToiMTExMTExMTExMTExMTExMTExMTExMTAwIiw3OiIxMTExMTExMTExMSIsODoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEiLDEzODoiMTExMTExMTExMTExMTExMTExMTExMTExMSIsMTM5OiIxMTExMTExMTExMTExMTExMTExMTExMTExIiwxNDA6IjExMTExMTExMTExMTExMTExMTExMTExMTEiLDE0MToiMTExMTExMTExMTExMTExMTExMTExMTExMSIsMTQyOiIxMTExMTExMTExMTExMTExMTExMTExMTExIiwxNDM6IjExIiw4MDoiMTExMTExMTExMTExMTExMTExMTExMTExIiw4MToiMTExMTExMTExMTExMTExMTExMTExMTExIiw4MjoiMTExMTExMTExMTExMTExMTExMTEwMTEwIiw4MzoiMTExMTExMTExMTExMTExMTExMDAwMDAwIiw4NToiMDExMTExMTExMTExMTExMTExMTExMTExIiw4NzoiMTExMTExMTEwMDExMTExMDExMTExMTExIiw4OToiMTExMTExMTEwMTEwMTAwMDAwMDAxMTExIiw5MDoiMTExMTEwMTExMTExMTExMTEwMDAxMTExMTAiLDkzOiIxMTExMTExMTExMTExMTExMTAwMDAxMTExIiw5NDoiMTExMTExMTExMTExMTExMTExMTExMTExMSIsOTY6IjExMTExMTExMTExMTExMTExMTExMTExMTEiLDk5OiIxMDAiLDEwMDoiMTExMTAxMTExMTExMTExMTExMCIsMTAyOiIxIiw0NDoiMTEiLDEwOToiMSIsNTM6IjExMTExMTExMTExMTExMTExMTExMTExMSIsNTQ6IjExMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiw1NzoiMDAwMDAwMDAwMDAwMDAwMDAwMDAxMDAwMDAwMDAiLDYyOiIxMTExMTExMTExMTExMTExMTExMTExMTEiLDYzOiIxMTExMTExMTExMTExMTExMTExMTExMTEifSwidGltZXN0YW1wIjoiMTc3NDIzNzEwMjQ5MCIsInRyYW5zQXV0aCI6ZmFsc2UsInR0bFZhbHVlIjowLCJ1aWQiOiI3NjA2MjMxOTEiLCJ1c2VyVHlwZSI6IkZSRUVJQUwiLCJ3aWZpbmRMaW1pdE1hcCI6e319fQ==.D1F0D33C1379DEDD30CE8078849A9C1AE5E6CA808EC337C79B66E2F96BF5DBEE'


class IFinDFuncInterval(enum.Enum):
    D = "D"
    W = "W"
    M = "M"
    Q = "Q"
    Y = "Y"


class IFinDFuncCPS(enum.Enum):
    NO_ADJUST = "1"
    QFQ = "2"
    HFQ = "3"
    VOLUME_QFQ = "4"
    VOLUME_HFQ = "5"
    BONUS_QFQ = "6"
    BONUS_HFQ = "7"


ADJUST_TYPE_MAP = {
    "": IFinDFuncCPS.NO_ADJUST.value,
    "qfq": IFinDFuncCPS.QFQ.value,
    "hfq": IFinDFuncCPS.HFQ.value
}


class IFinDFuncCurrency(enum.Enum):
    NONE = "YSHB"
    RMB = "RMB"
    USD = "MHB"
    HKD = "GHB"


CURRENCY_TYPE_MAP = {
    "": IFinDFuncCurrency.NONE.value,
    "rmb": IFinDFuncCurrency.RMB.value,
    "usd": IFinDFuncCurrency.USD.value,
    "hkd": IFinDFuncCurrency.HKD.value
}


def convert_function_params(options: QueryOptions) -> Dict[str, str]:
    func_params: Dict[str, str] = {
        "Interval": options.get("interval", IFinDFuncInterval.D.value),
        "CPS": IFinDFuncCPS.QFQ.value,
        "Currency": IFinDFuncCurrency.NONE.value,
        "Fill": "Blank",
    }
    func_params["CPS"] = ADJUST_TYPE_MAP[options.get("adjust", "")]
    # func_params["Currency"] = CURRENCY_TYPE_MAP[options.get("currency", "")]
    return func_params


class IFinDApi:
    _instance = None
    refresh_token: str
    access_token: str
    
    def __new__(cls, refresh_token: str | None = None):
        # 单例模式，保证全局只有一个
        if cls._instance is None:
            if not refresh_token:
                raise ValueError("Missing argument refresh_token!")
            cls._instance = super().__new__(cls)
            cls._instance.refresh_token = refresh_token
            cls._instance.access_token = cls._instance.get_access_token()

        return cls._instance

    @classmethod
    def instance(cls):
        return cls._instance
    
    def is_available(self) -> bool:
        return self.refresh_token is not None and self.access_token is not None

    def get_access_token(self):
        getAccessTokenUrl = 'https://quantapi.51ifind.com/api/v1/get_access_token'   # noqa
        getAccessTokenHeader = {"Content-Type": "application/json", "refresh_token": self.refresh_token}   # noqa
        getAccessTokenResponse = requests.post(url=getAccessTokenUrl, headers=getAccessTokenHeader)    # noqa
        accessToken = json.loads(getAccessTokenResponse.content)['data']['access_token']    # noqa
        if not accessToken:
            raise ValueError("Failed to get accessToken!")
        logger.info(f"got iFind access token: {accessToken}")
        return accessToken

    def get_realtime_quotation(self, codes: str, accessToken: str):
        thsUrl = 'https://quantapi.51ifind.com/api/v1/realtime__quotation'
        thsHeaders = {"Content-Type": "application/json", "access_token": accessToken}   # noqa
        thsPara = {"codes": codes, "indicators": "open,high,low,latest"}
        thsResponse = requests.post(url=thsUrl, json=thsPara, headers=thsHeaders)   # noqa
        # print(thsResponse.content)
        logger.debug(f"fetch {codes} realtime quotation: {thsResponse.content}")   # noqa
        return thsResponse

    def get_historical_data(
            self,
            codes: str,
            start: str,
            end: str | None = None,
            indicators: str = "open,close,high,low,volume,amount,changeRatio,turnoverRatio",
            func_params: Dict[str, str] = {
                "Interval": "D",
                "CPS": "2",
                "Currency": "RMB",
                "Fill": "Blank",
            }
        ) -> dict | None:    # noqa
        thsUrl = 'https://quantapi.51ifind.com/api/v1/cmd_history_quotation'
        thsHeaders = {"Content-Type":"application/json", "access_token": self.access_token}    # noqa
        thsPara = {
            "codes": codes,
            "indicators": indicators,
            "startdate": start,
            "enddate": end if end else datetime.now().strftime("%Y-%m-%d"),
            "functionpara": func_params
        }
        thsResponse = requests.post(url=thsUrl, json=thsPara, headers=thsHeaders)   # noqa
        # print(thsResponse.content)
        logger.debug(f"fetch {codes} historical data: {thsResponse.content}")    # noqa
        
        success, his_data = convert_to_df(thsResponse.content)
        if success:
            return his_data
        else:
            return None

