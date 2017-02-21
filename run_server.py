import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import os.path
import uuid
import json

import datetime
from datetime import timedelta

# import redis
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

from tornado.concurrent import Future
from tornado import gen
from tornado.options import define, options, parse_command_line

define("port", default=8080, help="run on the given port", type=int)
define("debug", default=False, help="run in debug mode")


"""
Instantiating MongoDB client

Using Mongo defaults, without security or any configurations, for demo purposes

"""

mongo = MongoClient()
db = mongo['mini_notifs']
notifications_collection = db.notifications
users_collection = db.users
push_notifications_collection = db.push_notifications


class Notifications(object):
	def __init__(self):
		# Maintain list of current clients
		self.connected = {}

	def wait_for_notifications(self, key=None):
		self.connected[key] = Future()
		return self.connected[key]

	def cancel_wait(self, key=None):
		del self.connected[key]


notifs = Notifications()

class BaseHandler(tornado.web.RequestHandler):
	def set_default_headers(self):
		self.set_header("Access-Control-Allow-Origin", "*")
		self.set_header('Access-Control-Allow-Methods', ' PUT, DELETE, OPTIONS')
		self.set_header("Access-Control-Allow-Credentials", "true")
		self.set_header("Access-Control-Allow-Headers", "Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers")

	def options(self):
		# no body
		self.set_status(204)
		self.finish()


class MessageUpdatesHandler(BaseHandler):
	@gen.coroutine
	def get(self):
		self.key = self.get_argument("key", None)
		print("GET says key is "+format(self.key))

		if not self.key:
			return

		self.future = notifs.wait_for_notifications(self.key)
		self.get_user_notifications(key=self.key, future=self.future)

		notifications = yield self.future

		if self.request.connection.stream.closed():
			return
		self.write(json.dumps(dict(notification=notifications)))

	def get_user_notifications(self, key=None, future=None):
		if key:
			user = users_collection.find_one({"current_key": key})
			user_notifications = notifications_collection.find({"to_user_id": user["_id"], "is_fresh": True})
			notifications = {}

			if not user_notifications:
				print("No notifications for "+format(user['name']))

			for notif in user_notifications:
				n = {}

				n["user_detail"] = users_collection.find_one({"_id": notif["from_user_id"]})
				n['post_id'] = str(notif['post_id'])

				n['html'] = tornado.escape.to_basestring(self.render_string("notification.html", notif=n))
				print("Sending: "+format(n['html']))

				# update the notification as delivered
				notifications_collection.update({"_id": notif["_id"]}, {"$set":{"is_fresh": False}}, upsert=False)
				future.set_result([n['html']])

		else:
			print("Without key")

	def post(self):
		self.key = self.get_argument("key", None)
		print("POST says key is "+format(self.key))

	def on_connection_close(self):
		notifs.cancel_wait(self.key)


"""
When triggered, checks for new notifications with 'is_fresh' as True,
for users in notifs.connected
"""
class NotificationsCruncher(BaseHandler):
	def get(self):
		print("Cruncher called!")
		auth = self.get_argument("auth", None)
		push_id = self.get_argument("push_id", None)

		# Not checking auth's value, just demo. Plus it should be encrypted too before being sent
		if not auth or not push_id:
			return

		# Should be expanded to cater to more than one push_notifications at a time
		try:
			pushed_notification = push_notifications_collection.find_one({"_id": ObjectId(push_id)})
		except:
			# push_id can be invalid
			return

		if pushed_notification:
			affected_users = pushed_notification['users']
			print("Pending push found, acting...")
		else:
			print("False alarm, errr push")
			return

		self.get_notifications_for_connected_users(affected_users)

		push_notifications_collection.update({"_id": pushed_notification["_id"]},{"$set":{"is_processed": True}})


	def get_notifications_for_connected_users(self, affected_users=None):
		connected = notifs.connected
		print("Users to notify: "+format(affected_users))

		for user_id in affected_users:
			try:
				user_data = users_collection.find_one({"_id": ObjectId(user_id)})
				key = user_data['current_key']
			except:
				# any issue with user_id or key
				continue

			print("Got the key! Attempting to push to "+format(user_data['name']))

			try:
				self.get_user_notifications(user=user_data, future=connected[key])
			except:
				# Keyerror is expected if user not connected
				continue

	def get_user_notifications(self, user=None, future=None):
		if user:
			user_notifications = notifications_collection.find({"to_user_id": user["_id"], "is_fresh": True})

			if not user_notifications:
				print("No notifications for "+format(user['name']))

			print(format(user['name'])+" is online and has some notifications waiting to be pushed")

			print("Notifications are: "+format(user_notifications))

			for notif in user_notifications:
				n = {}

				n["user_detail"] = users_collection.find_one({"_id": notif["from_user_id"]})
				n['post_id'] = str(notif['post_id'])

				n['html'] = tornado.escape.to_basestring(self.render_string("notification.html", notif=n))
				print("Sending from cruncher: "+format(n['html']))

				# update the notification as delivered
				notifications_collection.update({"_id": notif["_id"]}, {"$set":{"is_fresh": False}}, upsert=False)
				print("Sending notification")
				future.set_result([n['html']])
		else:
			print("Without key")
			return

def main():
	parse_command_line()
	app = tornado.web.Application(
		[
			(r"/mini_notif_server", MessageUpdatesHandler),
			(r"/mini_notif_server/push", NotificationsCruncher),
			],
		cookie_secret="PO_there_are_no_secrets",
		template_path=os.path.join(os.path.dirname(__file__), "templates"),
		static_path=os.path.join(os.path.dirname(__file__), "static"),
		xsrf_cookies=False,
		debug=options.debug,
		)
	app.listen(options.port)
	tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
	main()