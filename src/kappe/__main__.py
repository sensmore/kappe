import sys
from kappe import cli
from kappe.utils.error_handling import capture_exceptions

capture_exceptions()

if __name__ == '__main__':
    try:
        cli.main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)  
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)
