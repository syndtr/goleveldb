#! /usr/bin/env python
# encoding: utf-8
# Suryandaru Triandana, 2012 (syndtr@gmail.com)

PKG_PREFIX = 'github.com/syndtr/goleveldb/'

def listdir_node(node):
	lst = []
	for d in node.listdir():
		if d[:1] == '.':
			continue
		n = node.find_dir(d)
		if n:
			lst.append(n)
			lst += listdir_node(n)
	return lst

def build(ctx):
	for node in listdir_node(ctx.path):
		ctx.gopackage(source=[node], target='pkg/'+PKG_PREFIX+node.path_from(ctx.path), defer=True)
