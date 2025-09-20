def acked(err, msg):
    if err is not None:
        print(f"An error has occured: {err}")