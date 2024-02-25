def is_number(n):
    """
    Checks if variable is a number a string of a number
    """
    try:
        x = float(n)
        return True
    except:
        return False
    
def convert_cap_value(s):
    """
    converts market cap from yahoo finance into actual number,
    e.g. 212.689B = 212.689 Billions = 212.689 * 10^9
    
    input:
        - s: string
    output:
        - s: float
    """
    s = s.strip()
    d = {'B':10**9,'M':10**6}
    for k in d.keys():
        if k in str(s):
            s = float(s.replace(k,'')) * d[k]
            
    return s