import json
import socket
import sys
import threading

import requests


def send_tweets_to_spark(http_resp, tcp_connection):
    try:
        for line in http_resp.iter_lines():
            if line:
                json_response = json.loads(line)
                tweet_data = json_response['data']
                tcp_connection.send((json.dumps(tweet_data) + "\n").encode("utf-8"))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


class TweetGenerator:
    __localhost = "localhost"
    __twitter_stream_api = "https://api.twitter.com/2/tweets/sample/stream"
    __bearer_token = "AAAAAAAAAAAAAAAAAAAAAGiHkgEAAAAAdmv2aY2fZowkI9yOlHF64Azwo6g" \
                     "%3DKakPp8pFMREvacLFEbXTe5Uaf8zTYFRB9UuTzMVe3s0OkYlEFD "

    def __init__(self, server_port):
        self.__server_port = server_port
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__server_socket.bind((self.__localhost, self.__server_port))

    def __bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.__bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    def __get_tweets(self):
        params = {'tweet.fields': 'public_metrics,lang'}
        response = requests.get(
            self.__twitter_stream_api, auth=self.__bearer_oauth, stream=True, params=params
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        return response

    def start_server(self):
        while True:
            self.__server_socket.listen(1)
            print("Waiting for client connection...")
            conn, addr = self.__server_socket.accept()
            client = threading.Thread(target=self.__client_handler, args=(conn,))
            client.start()

    def __client_handler(self, conn):
        print("Handling new client on " + str(conn))
        response = self.__get_tweets()
        send_tweets_to_spark(response, conn)
