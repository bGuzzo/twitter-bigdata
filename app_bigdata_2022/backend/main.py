import threading

from app_bigdata_2022.backend.tweet_generator import TweetGenerator
from app_bigdata_2022.backend.tweet_processor import TweetProcessor
from app_bigdata_2022.frontend.controllers import app

if __name__ == '__main__':
    socket_port = 9004
    fe_port = 5001
    gen = TweetGenerator(socket_port)
    proc = TweetProcessor(socket_port, fe_port)
    thread_gen = threading.Thread(target=gen.start_server)
    thread_gen.start()
    thread_proc = threading.Thread(target=proc.start_processor)
    thread_proc.start()
    app.run(host='localhost', port=fe_port)
