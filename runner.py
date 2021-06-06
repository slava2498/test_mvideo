from app import create_app, parser

app, result = create_app()

if __name__ == '__main__':
	if not result:
		parser.start()

	app.run(debug=True)