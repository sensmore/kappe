__version__ = '0.21.0'
try:
    from kappe.utils.logging import setup_logging
    setup_logging()
except ImportError:
    pass
