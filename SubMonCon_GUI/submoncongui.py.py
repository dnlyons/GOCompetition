# -*- coding: utf-8 -*- 

###########################################################################
## Python code generated with wxFormBuilder (version Aug  8 2018)
## http://www.wxformbuilder.org/
##
## PLEASE DO *NOT* EDIT THIS FILE!
###########################################################################

import os
import wx
import wx.xrc


###########################################################################
# Class Frame1
###########################################################################

class Frame1(wx.Frame):

    def __init__(self, parent):
        wx.Frame.__init__(self, parent, id=wx.ID_ANY, title=u"Sub, Mon, Con File Selector", pos=wx.DefaultPosition, size=wx.Size(500, 217),
                          style=wx.DEFAULT_FRAME_STYLE | wx.STAY_ON_TOP | wx.TAB_TRAVERSAL)

        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)

        frameVsizer = wx.BoxSizer(wx.VERTICAL)

        self.panel1 = wx.Panel(self, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, wx.TAB_TRAVERSAL)
        Vsizer1 = wx.BoxSizer(wx.VERTICAL)

        Hsizer1 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_button11 = wx.Button(self.panel1, wx.ID_ANY, u".sub", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer1.Add(self.m_button11, 0, wx.ALL, 5)

        self.m_textCtrl1 = wx.TextCtrl(self.panel1, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer1.Add(self.m_textCtrl1, 1, wx.ALL, 5)

        self.m_button12 = wx.Button(self.panel1, wx.ID_ANY, u"Clear", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer1.Add(self.m_button12, 0, wx.ALL, 5)

        Vsizer1.Add(Hsizer1, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 5)

        Hsizer2 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_button21 = wx.Button(self.panel1, wx.ID_ANY, u".mon", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer2.Add(self.m_button21, 0, wx.ALL, 5)

        self.m_textCtrl2 = wx.TextCtrl(self.panel1, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer2.Add(self.m_textCtrl2, 1, wx.ALL, 5)

        self.m_button22 = wx.Button(self.panel1, wx.ID_ANY, u"Clear", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer2.Add(self.m_button22, 0, wx.ALL, 5)

        Vsizer1.Add(Hsizer2, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 5)

        Hsizer3 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_button31 = wx.Button(self.panel1, wx.ID_ANY, u".con", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer3.Add(self.m_button31, 0, wx.ALL, 5)

        self.m_textCtrl3 = wx.TextCtrl(self.panel1, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer3.Add(self.m_textCtrl3, 1, wx.ALL, 5)

        self.m_button32 = wx.Button(self.panel1, wx.ID_ANY, u"Clear", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer3.Add(self.m_button32, 0, wx.ALL, 5)

        Vsizer1.Add(Hsizer3, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 5)

        Hsizer4 = wx.BoxSizer(wx.HORIZONTAL)

        Hsizer4.Add((0, 0), 1, wx.EXPAND, 5)

        self.m_button_done = wx.Button(self.panel1, wx.ID_ANY, u"Done", wx.DefaultPosition, wx.DefaultSize, 0)
        Hsizer4.Add(self.m_button_done, 0, wx.ALL, 5)

        Hsizer4.Add((0, 0), 1, wx.EXPAND, 5)

        Vsizer1.Add(Hsizer4, 0, wx.EXPAND, 5)

        self.panel1.SetSizer(Vsizer1)
        self.panel1.Layout()
        Vsizer1.Fit(self.panel1)
        frameVsizer.Add(self.panel1, 1, wx.EXPAND | wx.ALL, 5)

        self.SetSizer(frameVsizer)
        self.Layout()

        self.Centre(wx.BOTH)

        # -- CONNECT EVENTS -----------------------------------------------------------------------
        self.m_button11.Bind(wx.EVT_BUTTON, self.m_button11OnButtonClick)
        self.m_textCtrl1.Bind(wx.EVT_TEXT, self.m_textCtrl1OnText)
        self.m_button12.Bind(wx.EVT_BUTTON, self.m_button12OnButtonClick)
        self.m_button21.Bind(wx.EVT_BUTTON, self.m_button21OnButtonClick)
        self.m_textCtrl2.Bind(wx.EVT_TEXT, self.m_textCtrl2OnText)
        self.m_button22.Bind(wx.EVT_BUTTON, self.m_button22OnButtonClick)
        self.m_button31.Bind(wx.EVT_BUTTON, self.m_button31OnButtonClick)
        self.m_textCtrl3.Bind(wx.EVT_TEXT, self.m_textCtrl3OnText)
        self.m_button32.Bind(wx.EVT_BUTTON, self.m_button32OnButtonClick)
        self.m_button_done.Bind(wx.EVT_BUTTON, self.m_button_doneOnButtonClick)

    # -- EVENT FUNCTIONS --------------------------------------------------------------------------
    # -- GET SUB FILENAME -------------------------------------------------------------------------
    def m_button11OnButtonClick( self, event ):
        wildcard = "Subsystem File (*.sub,*.txt)|*.sub;*.txt|" "All files (*.*)|*.*"
        dialog = wx.FileDialog(None, 'Select Sub File', os.getcwd(), "", wildcard, wx.FD_OPEN)
        dialog.Center(wx.BOTH)
        dialog.ShowModal()
        self.subfname = dialog.GetPath()
        self.m_textCtrl1.ChangeValue(os.path.split(self.subfname)[1])
        dialog.Destroy()

    # -- CLEAR SUB FILENAME -----------------------------------------------------------------------
    def m_button12OnButtonClick( self, event ):
        self.subfname = ''
        self.m_textCtrl1.ChangeValue('')

    # -- GET MON FILENAME -------------------------------------------------------------------------
    def m_button21OnButtonClick( self, event ):
        wildcard = "Subsystem File (*.mon,*.txt)|*.mon;*.txt|" "All files (*.*)|*.*"
        dialog = wx.FileDialog(None, 'Select Mon File', os.getcwd(), "", wildcard, wx.FD_OPEN)
        dialog.Center(wx.BOTH)
        dialog.ShowModal()
        self.monfname = dialog.GetPath()
        self.m_textCtrl2.ChangeValue(os.path.split(self.monfname)[1])
        dialog.Destroy()

    # -- CLEAR MON FILENAME -----------------------------------------------------------------------
    def m_button22OnButtonClick( self, event ):
        self.monfname = ''
        self.m_textCtrl2.ChangeValue('')

    # -- GET CON FILENAME -------------------------------------------------------------------------
    def m_button31OnButtonClick( self, event ):
        wildcard = "Subsystem File (*.con,*.txt)|*.con;*.txt|" "All files (*.*)|*.*"
        dialog = wx.FileDialog(None, 'Select Con File', os.getcwd(), "", wildcard, wx.FD_OPEN)
        dialog.Center(wx.BOTH)
        dialog.ShowModal()
        self.confname = dialog.GetPath()
        self.m_textCtrl3.ChangeValue(os.path.split(self.confname)[1])
        dialog.Destroy()

    # -- CLEAR CON FILENAME -----------------------------------------------------------------------
    def m_button32OnButtonClick( self, event ):
        self.confname = ''
        self.m_textCtrl3.ChangeValue('')

    # -- SUB FILENAME TEXTCONTROL -----------------------------------------------------------------
    def m_textCtrl1OnText(self, event):
        self.subfname = self.m_textCtrl1.GetValue()

    # -- MON FILENAME TEXTCONTROL -----------------------------------------------------------------
    def m_textCtrl2OnText(self, event):
        self.monfname = self.m_textCtrl2.GetValue()

    # -- CON FILENAME TEXTCONTROL -----------------------------------------------------------------
    def m_textCtrl3OnText(self, event):
        self.confname = self.m_textCtrl3.GetValue()

    # -- DONE BUTTON ------------------------------------------------------------------------------
    def m_button_doneOnButtonClick( self, event ):
        app.ExitMainLoop()


# -- INITIALIZE GUI ---------------------------------------------------------------------------
app = wx.App(redirect=False)
frame = Frame1(None)
frame.app = app
frame.Show(True)

# -- INITIALIZE FILENAMES ---------------------------------------------------------------------
frame.subfname = ''
frame.monfname = ''
frame.confname = ''

# GO TO GUI -----------------------------------------------------------------------------------
app.MainLoop()

# -- ASSIGN FILENAMES -------------------------------------------------------------------------
subfname = frame.subfname
monfname = frame.monfname
confname = frame.confname

# CLOSE GUI -----------------------------------------------------------------------------------
frame.Show(False)
frame.Close()

print(subfname)
print(monfname)
print(confname)
