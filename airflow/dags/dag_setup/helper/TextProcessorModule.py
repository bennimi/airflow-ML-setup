# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 00:01:53 2020

@author: Benny Mü
"""
import pandas as pd
import numpy as np
import re
import gensim
import gensim.utils
from gensim import corpora,models
import nltk
from dag_setup.helper.helper_functions import nltk_downloads
  
""" Error within packages normalise"""  
#from normalise import normalise

import spacy
#spacy.load('en')
from spacy.lang.en import English
from nltk.corpus import wordnet as wn
from nltk.tokenize import WordPunctTokenizer
from num2words import num2words
import sys, glob, os, zipfile, itertools, random , datetime, collections
from tqdm.notebook import tqdm


#parser.Defaults.stop_words |= {"my_new_stopword1","my_new_stopword2"} # add new stopwords #nlp.Defaults.stop_words.add("my_new_stopword")
#parser.Defaults.stop_words -= {"my_new_stopword1", "my_new_stopword2"} # remove stopwords #nlp.Defaults.stop_words.remove("whatever")



class Preprocess_Pipeline(): #BaseEstimator, TransformerMixin):
    """Availible functions: PrePreProcesor, Normalizer, PosCleaner, Tokenizer, transform. | Methods can be fitted and reused, stored as pipeline until triggered by transform method.\n
    Note that not every order of Processors make logically sense.\n
    PrePreProcessor: Takes raw data as string. Pre-filters emails,websites,encoding erros and punctuation. For customization check functon.\n
    Normalizer: Takes raw data as string. For Info, check Package´s github: https://github.com/EFord36/normalise \n
    PosCleaner: Takes raw data as string. Filters text by POS-tags. For customization check functon.\n
    Tokenizer: Takes raw data as string. Tokenizes and cleans token. Can return list of tokens or string. For more methods check function.\n
    transform: Has to be last step in Pipeline. Takes either tokens or string. Returns Series (for multiple line input) or string (for one line input).
    """
    safe_Pipline_stages = None
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.Pipline_stages = []
    def _check_input_type(self,data):
      if isinstance(data,pd.Series): return data, "pd.Series"
      elif isinstance(data,list): return pd.Series(data), "list" 
      elif isinstance(data,str): return data , "str" 
      else: sys.exit("Check input type. Pipeline takes str, list, pd.Series. Input is of tpye: {}".format(type(data)))
    

    def fit(self):
      Pipline_stages_fitted = self.Pipline_stages
      #return Pipline_stages_fitted
      return self

    def transform(self,data):
      #Pipline_stages_fitted = self.fit()
      Pipline_stages = self.Pipline_stages
      data, input_type  = self._check_input_type(data)
      if len(Pipline_stages) == 0: sys.exit("Define Pipeline stages (like 'Preprocess_Pipeline().PrePreProcessor().Tokenizer()') and before calling transform().")
      if input_type == "str":
        for stage in Pipline_stages:
            if stage == "PrePreProcessor": data = self._prepreprocessor(data,self.lower_case,self.letters_only,self.combined_pat)
            if stage == "Tokenizer": data = self._tokenizer(data, self.parser,  self.num_to_word, self.length_remove, self.stopword_remove, self.lemmatizer, self.to_sentence) 
            if stage == "PosCleaner": data = self._poscleaner(data,self.nlp,self.allowed_postags)
            if stage == "Normalizer": data = self._normalizer(data,self.variety,self.user_abbrevs)

      if (not input_type == "str") and (self.verbose == False):
        for stage in Pipline_stages:
            if stage == "PrePreProcessor": data = data.apply(lambda text: self._prepreprocessor(text,self.lower_case,self.letters_only,self.combined_pat))
            if stage == "Tokenizer": data = data.apply(lambda text: self._tokenizer(text, self.parser, self.num_to_word, self.length_remove, self.stopword_remove, self.lemmatizer, self.to_sentence))
            if stage == "PosCleaner": data = data.apply(lambda text: self._poscleaner(text,self.nlp,self.allowed_postags))
            if stage == "Normalizer": data = data.apply(lambda text: self._normalizer(text,self.variety,self.user_abbrevs))

      if (not input_type == "str") and (self.verbose == True):
        for stage in Pipline_stages:
            if stage == "PrePreProcessor":
              tqdm.pandas(desc=stage)      
              data = data.progress_apply(lambda text: self._prepreprocessor(text,self.lower_case,self.letters_only,self.combined_pat))
            if stage == "Tokenizer":
              tqdm.pandas(desc=stage) 
              data = data.progress_apply(lambda text: self._tokenizer(text, self.parser,self.num_to_word,self.length_remove, self.stopword_remove, self.lemmatizer, self.to_sentence))
            if stage == "PosCleaner":
              tqdm.pandas(desc=stage)
              data = data.progress_apply(lambda text: self._poscleaner(text,self.nlp,self.allowed_postags))
            if stage == "Normalizer":
              tqdm.pandas(desc=stage)
              data = data.progress_apply(lambda text: self._normalizer(text,self.variety,self.user_abbrevs))
      self.Pipline_stages
      return data 

    def PrePreProcessor(self,lower_case=True, letters_only=None,add_pat = None,combined_pat = {r"/(\r\n)+|\r+|\n+|\t+/": " ",r"www.[^ ]+": "",r"http[^ ]+": "",r"\S*@\S*\s?": "",r"[.]{2,}": r".", r"([a-z]+)([A-Z][a-z]+)": r"\1 \2"}):
      """letters_only: (None/True/False) None default. | add_pat: define an additional {pattern: substitute} combination to add to default. | \n 
      combined_pat: pass customized pattern as (regex list) or use default pattern, returns: string(s)."""    
      self.lower_case = lower_case
      self.letters_only = letters_only
      self.add_pat = add_pat
      self.combined_pat = combined_pat
      if not isinstance(self.combined_pat,dict): sys.exit("Pass combined_pat as dict of patterns and substitutes (see default pattern for logic).")
      if isinstance(self.add_pat,dict): self.combined_pat.update(self.add_pat)
      if not "PrePreProcessor" in self.Pipline_stages: self.Pipline_stages.append("PrePreProcessor")
      #return self #Preprocess_Pipeline(text)

    def _prepreprocessor(self,text,lower_case,letters_only,combined_pat):
      text = text.lower()
      for pat,sub in combined_pat.items(): 
        text = re.sub(r"{}".format(pat), r"{}".format(sub),text)
      text = re.sub("[+]", " plus", text)
      if letters_only == True: text = re.sub("[^a-zA-Z]", " ", text)
      if letters_only == False: text = re.sub("[^a-zA-Z0-9]", " ", text) 
      #print(text)    
      return text

    # def Normalizer(self,variety="BrE",user_abbrevs={}):
    #   """For Info, check Package´s github: https://github.com/EFord36/normalise."""
    #   self.variety = variety
    #   self.user_abbrevs = user_abbrevs
    #   if not "Normalizer" in self.Pipline_stages: self.Pipline_stages.append("Normalizer")
    #   #return self #Preprocess_Pipeline(text)

    # def _normalizer(self,text,variety,user_abbrevs):
    #     # some issues within normalise package
    #     try: return ' '.join(normalise(text,variety=self.variety,user_abbrevs=self.user_abbrevs,verbose=False))
    #     except NameError: 
    #       sys.exit("Check if Normalizer package is installed and imported as 'normalise'")
    #     else: return text

    def PosCleaner(self,spacy_model = "en_core_web_sm",allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV']):
      """spacy_model: takes string of model, for different language models. | allowed_postags: Define which Part of Speech should NOT be filtered.\n
      For more info, check spacy homepage"""
      self.nlp = spacy.load(spacy_model, disable=['parser', 'ner'])
      self.allowed_postags = allowed_postags
      if not "PosCleaner" in self.Pipline_stages: self.Pipline_stages.append("PosCleaner")
      #return self #Preprocess_Pipeline(text)

    def _poscleaner(self,text,nlp,allowed_postags):
      text = nlp(text)
      tokens = [token.lemma_ for token in text if token.pos_ in allowed_postags]
      return ' '.join(tokens)

    def Tokenizer(self,parser = English(), num_to_word = False, length_remove = False, stopword_remove = False, lemmatizer = False, to_sentence = False, **kwargs):
      """lower_case: True default | num_word, stopword_remove, lemmatizer, to_sentence: (True/False) False default |
      length_remove  False, or define int ("<=") as length to be removed | Praser spacy instance English() - for more info see spacy.io.\n
      returns: either (list of) tokens or string(s)."""
      #stopword_args = add/remove custom stopword
      self.parser = parser
      self.num_to_word = num_to_word
      self.length_remove = length_remove
      self.stopword_remove = stopword_remove
      self.lemmatizer = lemmatizer
      self.to_sentence = to_sentence
      if not "Tokenizer" in self.Pipline_stages: self.Pipline_stages.append("Tokenizer")
      #return self #Preprocess_Pipeline(text)

    def _tokenizer(self,text,parser,num_to_word,length_remove,stopword_remove,lemmatizer,to_sentence):
      tokens_list=[]
      tokens = parser(text)
      for token in tokens:
        if token.orth_.isspace(): continue
        if stopword_remove:
          if token.is_stop: continue
        #if lower_case: token = token.lower_
        #else: token = str(token)
        token = str(token)
        if num_to_word == True: 
          if token.isnumeric(): 
            try: token = num2words(token) # catch issues with num2word 
            except: pass
        if length_remove:
          if len(token) <= length_remove: continue
        if lemmatizer: 
          lemma = wn.morphy(token)
          if not lemma is None: token = lemma
        #if not isinstance(token,str):  print(type(token))
        tokens_list.append(token)
      if to_sentence: tokens_list = ' '.join(tokens_list)    
      return tokens_list