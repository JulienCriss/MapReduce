# coding=utf-8
"""
    Variabile globale
"""
import string

# mapeaza semnele de punctuatie
PUNCTUATION_TRANS = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
