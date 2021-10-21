import os 

dslFile = '# compiler settings shared between environments\n'\
        'baseUri = ${uri.workingDir}\n' \
        '# compiler-specific settings\n' \
        'system.folder.ruleset = ${baseUri}/ruleset\n' \
        '# override the online-config values with compiler-specific values\n' \
        'configVar.binaryFilesLocation = ${baseUri}\n' \
        'consoleVar.datacenter = "DUMMY_DATACENTER"'


def createEnvDslCompiler(env):
    envConf = '# envrironment-specific compiler settings \n' \
            'include required("../../jobsettings/'+env+'/r2r.conf")\n' \
            'include required("../../jobsettings/'+env+'/mee.conf")\n' \
            'include required("../dsl-compiler.conf")\n' \
            '# compiler-specific settings \n' \
            '# override the online-config values with compiler-specific values\n' \
            'schema.registry.path = ${baseUri}/avro //TODO: use here environment-specific URI to actual schema registry\n' \
            'schema.registry.url = null'  


    return envConf



def createCompileConfig(compile_path,compile_name):
    environments = ["acc","dev","prd","tst"]
    if not os.path.exists(compile_path):
        os.makedirs(compile_path)
    with open(os.path.join(compile_path,compile_name),"w") as conf:
        conf.write(dslFile)
        conf.close()
    print("Writing compile-config to "+ compile_path)
    for env in environments:
        path = os.path.join(compile_path,env)
        if not os.path.exists(path):
            os.makedirs(path)
        envConf = createEnvDslCompiler(env)
        with open(os.path.join(path,compile_name),"w") as conf:
            conf.write(envConf)
            conf.close()
        