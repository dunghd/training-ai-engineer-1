"""Package initializer: load local environment variables for local development.

This will load variables from `.env.local` then fallback to `.env` using python-dotenv.
It keeps behavior optional for production (if files don't exist, nothing changes).
"""
try:
	from dotenv import load_dotenv
	import os
	# prefer .env.local for local overrides, then .env
	here = os.path.dirname(__file__)
	load_dotenv(os.path.join(here, '..', '.env.local'))
	load_dotenv(os.path.join(here, '..', '.env'))
except Exception:
	# python-dotenv may not be installed in some environments; ignore silently
	pass