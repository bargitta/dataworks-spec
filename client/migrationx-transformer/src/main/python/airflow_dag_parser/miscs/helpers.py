def resolve_param_type_title(af_param):
    if af_param.schema is not None and 'type' in af_param.schema:
        param_type = af_param.schema['type']
    else:
        param_type = 'UNKNOWN'
    if af_param.schema is not None and 'title' in af_param.schema:
        param_title = af_param.schema['title']
    else:
        param_title = None
    return param_type, param_title