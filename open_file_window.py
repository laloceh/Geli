#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  2 10:31:47 2018

@author: eduardo
"""

def open_files():
    """Returns list of filenames+paths given starting dir"""
    import Tkinter
    import tkFileDialog

    root = Tkinter.Tk()
    root.withdraw()
    root.overrideredirect(True)
    root.geometry('0x0+0+0')
    
    root.deiconify()
    root.lift()
    root.focus_force()
    
    filenames = tkFileDialog.askopenfilenames(parent=root, title = "Open file")
    root.destroy()
    
    return filenames[0]

def save_to_files():
    """Returns list of filenames+paths given starting dir"""
    import Tkinter
    import tkFileDialog

    root = Tkinter.Tk()
    root.withdraw()
    root.overrideredirect(True)
    root.geometry('0x0+0+0')
    
    root.deiconify()
    root.lift()
    root.focus_force()
    
    #filenames = tkFileDialog.asksaveasfilename(initialdir = "/",title = "Select file",filetypes = (("text files","*.txt"),("all files","*.*")))
    filenames = tkFileDialog.asksaveasfilename(parent=root, title = "Save file as...")
    
    #filenames = tkFileDialog.askopenfilenames(parent=root)
    root.destroy()
    
    return filenames
 
if __name__ == "__main__":
    
    inputfile = open_files()
    print inputfile
    
    outputfile = save_to_files()
    print outputfile