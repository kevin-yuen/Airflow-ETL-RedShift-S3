from airflow.sdk import Variable


class RedshiftVariableManager():
    def get_iam_role(self, iam_role_key): 
        return Variable.get(iam_role_key)

    def get_mapping_config(self, mapping_key):
        # get column mapping config for staging data
        return Variable.get(mapping_key)
    
    def get_ds_name(self, ds_key):
        # get staging dataset name
        return Variable.get(ds_key)