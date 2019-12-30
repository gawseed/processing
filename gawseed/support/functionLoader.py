import zipimport
import importlib
import inspect
import sys

saved_import_from_zip = None

def reset_saved_import_from_zip():
    global saved_import_from_zip
    saved_import_from_zip = None

def load_function(function_name,
                  default_module = 'gawseed.algorithm.generic',
                  import_from_zip = None,
                  save_import_zip = True):
    global saved_import_from_zip

    try:
        lastdotnum = function_name.rindex('.')
        modulename = function_name[:lastdotnum]   # just the module name
        fn_name    = function_name[lastdotnum+1:] # just the function name
    except: # assume they want gawseed.algorithm.generic
        modulename = default_module
        fn_name = function_name

    if import_from_zip or saved_import_from_zip:
        # if it isn't a list, turn it into one
        if import_from_zip and type(import_from_zip) == type(str()):
            import_from_zip = [import_from_zip]

        # zip file module names must be / separated
        modulename = modulename.replace(".","/")
        mod = None

        # for each zip file in a list, try to load it until we find it
        zip_list = import_from_zip or saved_import_from_zip

        for zip_file in zip_list:
            importer = zipimport.zipimporter(zip_file)

            # see if the module contains the file
            if importer.find_module(modulename):
                mod = importer.load_module(modulename)
                continue
                
        # save the list if requested
        if import_from_zip and save_import_zip:
            saved_import_from_zip = import_from_zip

    else:
        mod = importlib.import_module(modulename)

    if not mod:
        sys.stderr.write("Failed to load module '%s'\n  function_name: %s\n" % (modulename, function_name))
        exit(1)

    # see if we need to be recursively called by this module who needs to load even more

    if not hasattr(mod, fn_name):
        raise ValueError("Failed to find function '%s' in module '%s'\n function_name: %s\n" %
                         (fn_name, modulename, function_name))

    if hasattr(mod, 'gawseed_load_functions'):
        mod.gawseed_load_function = load_function
        mod.gawseed_load_functions()

    return getattr(mod, fn_name)



