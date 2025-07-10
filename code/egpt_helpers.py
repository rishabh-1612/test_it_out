import requests
import json
from analyser_repo_pipeline.egpt_helpers import login_to_egpt
from logger import info_logger, error_logger
import config
import requests
import json
import time
from tenacity import retry, stop_after_attempt, wait_exponential


def sync_github_egpt_call(task_id, github_url, branch_name, assistant_id):
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda retry_state: None,
    )
    def _make_sync_request(token, url, headers, payload):
        response = requests.request("POST", url, headers=headers, data=payload)
        info_logger.info(f"Response: {response.status_code} {response.text}")
        response.raise_for_status()
        return response

    max_login_attempts = 3
    login_attempt = 0
    token = None

    while login_attempt < max_login_attempts and token is None:
        try:
            token = login_to_egpt()
            if token:
                token = "Bearer " + token
            else:
                login_attempt += 1
                if login_attempt < max_login_attempts:
                    info_logger.warning(
                        f"Login attempt {login_attempt} failed, retrying in {2**login_attempt} seconds..."
                    )
                    time.sleep(2**login_attempt)
                else:
                    error_logger.error(
                        "Max login attempts reached. Unable to login to EGPT"
                    )
                    return None
        except Exception as e:
            error_logger.error(f"Login error: {str(e)}")
            login_attempt += 1
            if login_attempt >= max_login_attempts:
                return None
            time.sleep(2**login_attempt)

    info_logger.info("Successfully logged in to EGPT")

    info_logger.info(f"Now syncing project with EGPT")
    url = f"{config.EGPT_DOMAIN}/api/chatbot/syncNow"
    payload = json.dumps(
        {
            "contexts": {
                "taskId": task_id,
                "contentType": "github",
                "repos": [
                    {
                        "name": "github_repo_" + task_id,
                        "mimeType": "application/vnd.github.repository",
                        "webViewLink": github_url,
                        "id": "19223750-ca41-4-8--0.7625869538852184",
                        "batchSize": "10",
                        "branch_name": branch_name,
                    }
                ],
            },
            "type": "github",
            "sheet_url": [],
            "name": assistant_id,
            "organizationName": "84lumber",
            "requestId": task_id,
        }
    )

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "authorization": token,
        "content-type": "application/json",
        "cookie": "cookie-consent=true; _ga=GA1.1.195348623.1734422370; _ga_YGGCXJM075=GS1.1.1734422370.1.1.1734422397.0.0.0; next-auth.csrf-token=19db1342a692f3188d39fcf2ff6471056e93ea2a47b7fca4dfab846d1f2e2543%7C7c376d39d8283cd188da32919ba021fc7dda1b542c2b04f759616c8ef675bbfd; __Secure-next-auth.callback-url=https%3A%2F%2Fappsecauth-ai.techo.camp; __Secure-els-.next-auth.csrf-token=c1592c4908ee2cc46094d861203cb9521675a6a450742655f6cafb94538eb59d%7Ccf5334f34d57ee990e6fa714103e5c189d1cfa1160b3b9ccea6d5d46c90be969; __Secure-els-.next-auth.callback-url=https%3A%2F%2Fdev-84lumber-ai.techo.camp%2F; Enterprise-GPT-Maker-84lumber-appmod4fa59dev=84lumber-appmod4fa59dev-71771fc6-cc2e-4474-9146-4a714df50dbc; Enterprise-GPT-Maker-84lumber-architecturegen=84lumber-architecturegen-2b648216-ee77-45a4-b458-cebbd361c311; email=anil.phiyak@techolution.com; userId=6620d229d536a41479216dbf; Enterprise-GPT-Maker-84lumber-newingestiontest=84lumber-newingestiontest-691bf124-5693-4650-925e-3b2a6b469746; Enterprise-GPT-Maker-84lumber-cbasictopython=84lumber-cbasictopython-271d6250-1a99-4fb9-97a8-5b7e74fd1f54; __Secure-next-auth.session-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..LodO9hZBkXygzNiF.Xe_I6tlLQbhQs6OFTvsMtdySuMsqBAXlUB5cux6KvVxQx7chDHjWUOWEjAhEK71TGACFmEUKo7mR_3t5-tau22cAoHIbJXfIS1NUcc4sounLONvtiZ_BzpuBuSg3LBbFsAZJ55T3p_fTHxnlahQlXSnxc08buL6rtubFywV3NXQNomRq5yBJLjUuGSongJdrrlrD2Uh1U3u2QGo5F_zrfzqQwxoCoQJeiLisZvCXBglrEORtBlYe1UCIpnXydqGtvvCkNGgxghZ-e0eNb9cgsMceJ57arzheeCdJBRVxilA3WMmXZe-z6oIezYPcqx3qpwlSpuyuQ7fq_OXiuYod3wIdO6iuvu7EwuGKuKWnuu-uYmzOriTgyqOc0LsT_P4MpRXLMQE5PseryvlJTQi-kqaiso3AeH8UlyYLlpAh-ry4ZaTkKMzJApnGt-swfvLzTjSP5a8nMo1US_n3U4pFvB6aKR3x3k-D8t5OEUPEbYYLwbEzEJEmFZiusbHbGy7v-twgCKSVDp6_PpTpOwM_1uCK4HVocxdX3EN8tOfXodMHqgEgthbzPgpcYNoJ7IFIKcKqqfrARzbFQE0raxfnTDAgD2p0RHIVpG8gTcCQ42Pk9AGiHxg9-lJKQieDo7EYtp6oiLzA-AjWkfAgGS_6g7CrMU2msoa3TqSwBYU_MUwvl7km-L6HXYnlPZmQkmIdd1qZEEWh2DSR-mpzGIWZ057LxCTogZfnzpumNpmEv9ZrLV6aHJCz_AWAAtVM-XYROYphIhnDPAgTjeevxWXtEdmw0edLA4OfIQwwTeik3PwaaTETCwl1koIwbj15atRfuFXA8lYeqhUhgAf8ZY060KXpmt1QAXNlqO2ahTa6SpWS9qnQxyWPU_nSiYZZj8Fdsi6v3rsp6E5ookIlym4c6jNmpLT9OaOv68PAbZ57RxLSekUyzhCgMHL1t3xMeSDy2EEco81X7AN6FzFDvEduLkdIr51ULRj2RyQQAXcfpwlUmUaJ-42bWnWuLFcw7fMzriIC42aeuRcT3LLpl3R3C1wwI-vBWIc3EOgLJRK3fnYoexdak3q5zUWw1R6G9syEoxZR753JujA2ZJNZVdH5mfPiNboZ8qeWxIyPACA1bAMSAubbSmSV2HVHyF4bfKwq0h6IGtF1cUQjg79gTwAf3u12q84sNB7T9zBPM0j6NlO052K8dklzEXQ8a-khQx8XCbiP4fJs8Un9NjbCZ8pjYW3B79sP68bq6Tzgabcr2zbvqY603XsNaqcF9Ev6MM8j74rYjeMLtSZara3lmnpWVoPC1YnXGp9RkBZdxYYPxb7HC8rnHxv28t8JIBV25Oc4XOOKNnjS-KTpK4DqRChmVH1BlrOM_vrECZ6_C5qM91ZTbNriXUUixKqj75eZJ13Z7Y3W2Iip4VSzbkBu1iHXF74Xu2eOCCbZyeyKSeOGI9elnlZHVsHoRAA7qyZHmEh7ehi-FQjsbOQr-agvHDbbYHWWYpRmDkk_Pz5devd2u8pv0A.9jtyvK_QqswZtonWJ6Hgeg; __Secure-els-.next-auth.session-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..62ZFmdl2psIREOJw.p0kx8zjpSGP8EGD1DYxOdUHWh8pU5Z6leaVr1VwjVmZNuXvZy_4MplfCr1HekSBWNao4UTDWqDLg_Lx66vYbD1VFo0a5W59hKw_zjohOkFflvUbNZmujyt15700DDYdacpHhdQirX6PKfjywXbtrJ3lx--KxY4nUiZnjpYCjMld3MYD12xM8mrZfulKADrprapuBHTMdasE9-sGGBqn-_nCVEB6oaHqvQu9Lt2cC2VOZUDq60C9gyIJbFDEf1FJk5G77Q3qDRpnki_jYy1fl820Z1IhrQl0DQBxIRlFqirgB0TvOSqeJ2zepmEIb2CChu-ecTowXpRZ1u9ijewE35FSmY4rHcQ1VoSHNb7DHbKFScl6mFwWS8PGTaree_c_abTJGYiLHoDNjlgbWtHtnrwuH0nGOx--zMJ9mM5W14UU-Vxfe2HNHKyTNHyetlho5IyOm2-EP_87xAuwyaanPeqlAiuWn940syXBFg6UsCbdGOsyErtTAA-zFOiIuQKvHJkSs_xoJKW5-sLRsaVodiM2lrG-LKC8rBZwryyARSyoOuxETWi-InsxQsfJjSf41SNe5ZB2S6ktcngUiK2xZCLuAH9Ac6eX8FPdmXe9a8ApyAW-MRVhqqKjkQsR8T8l6ueLbufrH82HRv3vjJSHTDk_WAG4ptoSzZPpS1gh19DaQiL-H_k9cbhMLiRYhrUtpKYTSKQmTzd-G7Lx39kLtSkFYk-4Ealm7gP8aGOHxz-IwX2tGXKTz8lzaIY2_xeJGWGliVGr4DW2EzHxVV25SnxdDYJcRjClNqy_Nww300ylxDTNk7t67cixtOSVKKM41bVT1D5e27SW3DtyLO8rY8dxwfftZPNfGE8aLGT1DXiTGlRgTY7bGEYnh.-u6dPD_Ynua0yBL6bqicQQ",
        "origin": config.EGPT_DOMAIN,
        "priority": "u=1, i",
        "referer": "https://dev-84lumber-ai.techo.camp/dashboard?name=newingestiontest&organizationName=84lumber&tabIndex=0",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }

    info_logger.info(f"Request: {url} \n headers: {headers} \n Payload : {payload}")

    try:
        response = _make_sync_request(token, url, headers, payload)
        info_logger.info(f"Response: {response.text}")
        return response.text, response.status_code
    except requests.exceptions.RequestException as e:
        error_logger.error(f"Error making sync request: {str(e)}")
        return None, response.status_code
    except Exception as e:
        error_logger.error(f"Unexpected error during sync: {str(e)}")
        return None, None
