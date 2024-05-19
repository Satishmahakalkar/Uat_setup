import os

if os.getenv('PRODUCTION'):
    from .production import *
else:
    from .dev import *