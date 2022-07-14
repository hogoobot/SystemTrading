import requests

TARGET_URL = 'https://notify-api.line.me/api/notify'


def send_message(message, token=None):
    try:
        response = requests.post(
            TARGET_URL,
            headers={
                'Authorization': 'Bearer ' + token
            },
            data={
                'message': message
            }
        )
        status = response.json()['status']

        if status != 200:
            raise Exception('Fail need to check. Status is %s' % status)

    except Exception as e:
        raise Exception(e)
