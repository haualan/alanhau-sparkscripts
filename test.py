#!/usr/bin/python
# -*- coding: utf-8 -*-

a = "apple  1858  3 2"
a1 = (a.split())

def split_and_cast(x):
  r = x.split()[0:3]
  r = tuple([r[0],int(r[1]),int(r[2])])
  return r

print split_and_cast(a)