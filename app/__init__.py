import os
import sys

from flask import Flask
from app.main import views

def create_app():
	for x in ["app/static/csv", "app/static/result"]:
		if not os.path.isdir(x):
			os.mkdir(x)

	result = os.path.isfile('app/static/result/file.json')
	csv_file = os.path.isfile('app/static/csv/recommends.csv')

	if not csv_file and not result:
		raise RuntimeError('не найден файл csv')

	app = Flask(__name__)
	app.add_url_rule('/<string:slug>/', 'slug', views.get_product, methods=['GET'])
	app.add_url_rule('/<string:slug>/<float:recom>', 'slug&recom', views.get_product, methods=['GET'])
	
	sys.path.append("../config.py")
	return app, result
