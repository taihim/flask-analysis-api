from sqlalchemy import desc, and_, MetaData, Table 
from sqlalchemy.sql import func


class DBClient():
    def __init__(self, db):
        self._metadata = MetaData(bind=db.engine)
        self._table = Table('sample_data', self._metadata, autoload=True)
        self._session = db.session
        
        self._filter_list = ['date_to', 'date_from', 'country', 'os', 'channel']

        self._results = None
        self._query = None

    def __repr__(self):
        return "<DBClient Object> Represeting query: {}".format(self.get_query_str())

    def _get_metrics(self, params):
        return params['required_metrics'].split(',')

    def _get_filters(self, params):
        return {filter:params[filter] for filter in params.keys() if filter in self._filter_list}

    def _get_group_by(self, params):
        return params['group_by'].split(',')

    def _get_order_by(self, params):
        order_by_list = params['order_by'].split(',')
        order_by_dict = {}

        for order_str in order_by_list:
            split_str = order_str.split('/')
            if len(split_str) > 1: 
                order_by_dict[split_str[0]] = split_str[1] 
            else:
                order_by_dict[split_str[0]] = 'asc' 

        return order_by_dict

    def _prepare_data(self, params):
        arg_dict = {}
        
        arg_dict['required_metrics'] = self._get_metrics(params)        
        arg_dict['filters'] = self._get_filters(params)
        arg_dict['group_by'] = self._get_group_by(params)
        arg_dict['order_by'] = self._get_order_by(params)
        
        return arg_dict

    def _generate_query_args(self, arg_dict):
        query_args = []

        for group in arg_dict['group_by']:
            query_args.append(self._table.columns[group].label(group))

        for metric in arg_dict['required_metrics']:
            if metric == 'cpi':
                query_args.append((func.sum(self._table.columns['spend'])/func.sum(self._table.columns['installs'])).label(metric))
            else:
                query_args.append(func.sum(self._table.columns[metric]).label(metric))

        return query_args

    def _apply_filters(self, arg_dict):
        for i in arg_dict['filters'].keys():
            if i == "date_from": 
                self._results = self._query.filter(self._table.columns['date'] >= arg_dict['filters'][i])
            elif i == "date_to": 
                self._results = self._results.filter(self._table.columns['date'] <= arg_dict['filters'][i])
            else:
                self._results = self._results.filter(self._table.columns[i] == arg_dict['filters'][i])

    def _apply_group_by(self, arg_dict):
        for val in arg_dict['group_by']:
            self._results = self._results.group_by(self._table.columns[val])

    def _apply_order_by(self, arg_dict):
        for i in range(len(arg_dict['order_by'].keys())):
            if list(arg_dict['order_by'].keys())[i] in arg_dict['required_metrics']:
                if list(arg_dict['order_by'].keys())[i] == 'cpi':
                    if list(arg_dict['order_by'].values())[i] == "desc":
                        self._results = self._results.order_by(desc(func.sum(self._table.columns['spend'])/func.sum(self._table.columns['installs'])))
                    else:
                        self._results = self._results.order_by(func.sum(self._table.columns['spend'])/func.sum(self._table.columns['installs']))
                else:
                    if list(arg_dict['order_by'].values())[i] == "desc":
                        self._results = self._results.order_by(desc(func.sum(self._table.columns[list(arg_dict['order_by'].keys())[i]])))
                    else:
                        self._results = self._results.order_by(func.sum(self._table.columns[list(arg_dict['order_by'].keys())[i]]))
            else:
                if list(arg_dict['order_by'].values())[i] == "desc":
                    self._results = self._results.order_by(desc(self._table.columns[list(arg_dict['order_by'].keys())[i]]))
                else:
                    self._results = self._results.order_by(self._table.columns[list(arg_dict['order_by'].keys())[i]])

    def _jsonify_result(self, results):
        jsonified_result = {}

        jsonified_result['columns'] = [col['name'] for col in results.column_descriptions]
        jsonified_result['data'] = [dict(i) for i in results.all()]
    
        return jsonified_result

    def get_query_str(self):
        return str(self._results)

    def get_results(self, params):
        arg_dict = self._prepare_data(params)
        query_args = self._generate_query_args(arg_dict)

        self._query = self._session.query(*query_args)
        self._apply_filters(arg_dict)
        self._apply_group_by(arg_dict)
        self._apply_order_by(arg_dict)

        return self._jsonify_result(self._results)
        