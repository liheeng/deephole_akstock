import time

def retry(max_retries=3):

    def decorator(func):

        def wrapper(*args, **kwargs):

            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    time.sleep(2 ** i)

            return None

        return wrapper

    return decorator
