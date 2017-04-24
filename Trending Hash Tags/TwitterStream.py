import sys

import socket
import json

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

class TweetsListener(StreamListener):

	def __init__(self, socket):
		print "Tweet listener initialized"
		self.client_socket = socket

	def on_data(self, data):
		try:
			jsonMessage = json.loads(data)
			message = jsonMessage['text'].encode('utf-8')
			print message
			self.client_socket.send(message)
		except BaseException as e:
			print("Error on_data: %s" % str(e))
		return True

	def on_error(self, status):
		print status
		return True

def connect_to_twitter(connection):
	api_key,api_secret,access_token,access_token_secret = sys.argv[1:]

	auth = OAuthHandler(api_key, api_secret)
	auth.set_access_token(access_token, access_token_secret)

	twitter_stream = Stream(auth, TweetsListener(connection))
	twitter_stream.filter(track=['#'])


if __name__ == "__main__":

	s = socket.socket()
	host = "localhost"
	port = 7777
	s.bind((host, port))

	print("Listening on port: %s" % str(port))

	s.listen(5)

	connection, client_address = s.accept()

	print( "Received request from: " + str(client_address))

	connect_to_twitter(connection)














