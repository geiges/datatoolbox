#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 17:21:28 2020

@author: ageiges
"""

def create_personal_setting(modulePath, OS):
    
    # Linux 
    if OS == 'Linux':
        import tkinter as tk
        from tkinter import simpledialog, filedialog
        import os
        
        ROOT = tk.Tk()
        ROOT.withdraw()
        userName = simpledialog.askstring(title="Initials",
                                          prompt="Enter your Initials:")
        print("Welcome", userName)


        root = tk.Tk()
        root.withdraw() #use to hide tkinter window
        
        def search_for_file_path ():
            currdir = os.getcwd()
            tempdir = filedialog.askdirectory(parent=root, initialdir=currdir, title='Please select a directory')
            if len(tempdir) > 0:
                print ("You chose: %s" % tempdir)
            return tempdir

        file_path_variable = search_for_file_path()
        
    else:
        userName = input("Please enter your initials")
        file_path_variable = input("Please enter path to datashelf")
    
    if not file_path_variable.endswith('/'):
            file_path_variable = file_path_variable + '/'
    print ("\nfile_path_variable = ", file_path_variable)
    
    fin = open(modulePath + 'data/personal_template.py', 'r')
    
    fout = open(modulePath + 'personal.py', 'w')
    
    for line in fin.readlines():
        outLine = line.replace('XX',userName).replace('/PPP/PPP', file_path_variable)
        fout.write(outLine)
    fin.close()
    fout.close()

#%%

if __name__ == '__main__':
    create_personal_setting()