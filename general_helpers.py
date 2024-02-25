def is_number(n):
    """
    Checks if variable is a number a string of a number
    """
    try:
        x = float(n)
        return True
    except:
        return False
    