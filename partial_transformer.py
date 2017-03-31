from fuel.transformers import Transformer


class PartialTransformer(Transformer):
    '''
    transformer that allows child transformers manipulation with any data
    '''

    def __init__(self, data_stream, from_cols, to_cols, produces_examples=False):
        '''

        :param data_stream: 
        :param from_cols: names of the columns to be transformed
        :param to_cols: names of the columns to which the output is written
        '''
        super().__init__(data_stream=data_stream, produces_examples=produces_examples)
        self.from_inputs = from_cols
        self.to_inputs = to_cols


    def transform_batch(self, batch):
        '''
        perform the run() method on the columns of the 'batch' specified in self.from_columns 
        :param batch:  
        :return: 
        '''
        all_inputs = self.data_stream.dataset.provides_sources
        from_inputs_indeces = [all_inputs.index(input) for input in self.from_inputs]
        selected_inputs = tuple(batch[i] for i in from_inputs_indeces)

        transformed_selected_inputs = self.run(selected_inputs)
        assert len(transformed_selected_inputs) == len(self.to_inputs)
        assert type(transformed_selected_inputs) == type(batch)

        transformed_batch = tuple(transformed_selected_inputs[self.from_inputs.index(input)]
                                  if input in self.from_inputs
                                  else batch[i]
                                  for i, input in enumerate(all_inputs))

        assert len(transformed_batch) == len(batch)
        return transformed_batch

    def run(self, batch_data_to_be_transformed):
        '''here a transformation can be specified'''
        return batch_data_to_be_transformed



