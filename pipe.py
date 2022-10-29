"""
Module to build chain the execution of a series of functions
"""

import yaml, json
class Pipe:
    """
    class for composing functions
    
    """
    def __init__(self,functions,function_args=[],function_kwargs=[]):
        """

        Parameters
        ----------
        functions : list of callables (the functions to chain)
        
        
        function_args : list of tuples that are the arguments of the function, optional
                    if set, then should be same len as functions
        function_kwargs : list of dict that are the functions kwargs for running
        
        Warning
        -------
        Assumes the output of each function is the argument of the next one

        Returns
        -------
        None.

        """
        self.functions=functions
        self.__init_args(function_args)
        self.__init_kwargs(function_kwargs)
        
    def __init_args(self,function_args):
        """
        

        Parameters
        ----------
        function_args : tuple/list
            the list of args for a given function

        Raises
        ------
        ValueError
            raised if the len of args is not consistent with the number of functions
            in the pipeline.

        Returns
        -------
        None. 
        Initializes self.__init_args

        """
        if len(function_args)==0:
            self.function_args=[[]for _ in self.functions]
        elif len(function_args)!=len(self.functions):
            raise ValueError("if setting function arguments for the pipe,\
                                ensure function_args has same len as functions")
        self.function_args=function_args
    def __init_kwargs(self,function_kwargs):
        """
        

        Parameters
        ----------
        function_args : tuple/list
            the list of args for a given function

        Raises
        ------
        ValueError
            raised if the len of args is not consistent with the number of functions
            in the pipeline.

        Returns
        -------
        None. 
        Initializes self.__init_args

        """
        if len(function_kwargs)==0:
            self.function_kwargs=[{}for _ in self.functions]
        elif len(function_kwargs)!=len(self.functions):
            raise ValueError("if setting function arguments for the pipe,\
                                ensure function_kargs has same len as functions")
        else:
            self.function_kwargs=function_kwargs
        
    def __process(self,f,x,*args,**kwargs):
        return f(x,*args,**kwargs) if type(x)!=tuple \
            else f(*(list(x)+list(args)),**kwargs)
        
    def __call__(self,*args,**kwargs):
        args0 = list(args)+list(self.function_args[0])
        kwargs0=self.function_kwargs[0]
        kwargs0.update(kwargs)
        proc = self.functions[0](*args0,**kwargs0)
        for f,args,kwargs in zip(self.functions[1:],
                            self.function_args[1:],
                            self.function_kwargs[1:]):
            proc=self.__process(f,proc,*args,**kwargs)
            
        return proc
    @staticmethod
    def _parse_dict(d):
        """
        functions,function_args,function_kwargs for instantiating the PipeClass
        using a dictionary
        """
        return list(map(lambda k:(eval(k),[],d[k]),d.keys()))
    
    @classmethod
    def from_dict(cls,config_dict):
        """
        instantiate the class from a dictionary
        first step for instantiating from a file (json, yml ...)
        """
        functions,args,kwargs=list(zip(*Pipe._parse_dict(config_dict)))
        return cls(functions,args,kwargs)
    @staticmethod
    def __removeNoneValues(input_dict):
        output_dict=input_dict
        for k in input_dict.keys():
            if input_dict[k] is None:
                output_dict[k]={}
        print(output_dict)
        return output_dict
    @classmethod
    def from_json(cls,path):
        with open(path,'r') as file:
            input_dict=json.load(file)
        return cls.from_dict(input_dict)
    @classmethod 
    def from_yml(cls,path):
        #TODO
        raise NotImplementedError('todo')