def get_fld(flds, ns, default = None):        
        for fld in flds:
            if fld[0] == ns:
                v = fld[2:].strip()
                return v
        return default            


def get_fld_and_index(flds, ns, default = None):        
        idx = 0
        for fld in flds:
            if fld[0] == ns:
                v = fld[2:].strip()
                return v,idx
            idx += 1    
        return default, None           


