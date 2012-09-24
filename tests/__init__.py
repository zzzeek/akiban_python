from functools import update_wrapper

def fails(description):
    def decorate(fn):
        def go(*arg, **kw):
            try:
                fn(*arg, **kw)
                assert False, "Test did not fail as expected"
            except AssertionError:
                assert True
        return update_wrapper(go, fn)
    return decorate
