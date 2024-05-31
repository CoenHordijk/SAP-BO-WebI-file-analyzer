import xml.etree.ElementTree as ET
from lxml import etree
import re
import zipfile as zf
import json

import pandas as pd # om ooit naar excel te exporteren


# Constants
coding =  'latin1'
vartypes = ( 'Dimension', 'Measure', 'Detail')


# Bestandsnamen in het .WID zip archief
CNdpvarfilename = 'DP_Generic'
CNdocvarfilename =  'Data/C3/DocumentVariable'
CNsyncvarfilename =  'Data/C3/DATAPROVIDERS/SynchroManager'
CNdocspecfilename =  'Data/RE/DOCSPEC'
CNdocpropsfilename =  '_PROPERTIES'
CNdocpropsstartposlen = 8
CNdocpropsstartlocxml = 116

#regex om grenzen van variabelen te bepalen
CNnovarcharacter = '[^A-Z^a-z^0-9].'
CNnovarcharacter_or_end = '([^A-Z^a-z^0-9]|$)'

# output column seperator
CNseperator = ';'

# CCH for some XML tags we want to display it's instance name
ShowTagNameList = { 'REPORT','ALERTER', 'VTABLE',  'SECTION'}

# CCH some detail XML tags are not adding much information about the variable location
# so the shown XML path is not extended after these tags
FreezePathList = { 'ALERTER', 'VTABLE'}

#valid start and ending chars around var ID's in docspec XML
CNValidStartChars = { '"', 'o' }
CNValidEndChars = { '<', '"', '$', ','}


def BinaryFileViewer(myfile, exportfilename, verbose):
# CCH for analysis of binary files. Dumps binary content to formatted textfile
    f = open(myfile, mode="rb")
    varfile = f.read()
    f.close()
    
    flatfile = open( exportfilename, mode =  'a')
    
    for i in range(len(varfile)):
        flatfile.write(str(i) +  '\t' + hex(varfile[i]) + '\t' + str(varfile[i]) + '\t\n')

        if verbose:
            print(str(i) +  '\t' + hex(varfile[i]) + '\t' + str(varfile[i]) + '\t')
            
    flatfile.close()

def ReadDPFileUnidentifiedBlocks(dataproverfile, startpos):
# Functie tbv parsen van de dataprovider file
    
    startpos = startpos + 18 #Onbekende bytes

    # De volgende 4 bytes geeft aantal blokken van 17 bytes dat er op volgt weer.
    # Wat er in deze blokken staat is niet duidelijk
    bt_cntblocks = dataproverfile[startpos:startpos+3][::-1]
    cntblocks = int(bt_cntblocks.hex(), 16)
    # print(startpos, cntblocks)
    
    return startpos + 17 * cntblocks + 4


def ParseFormula(formula):
# Een formule bestaat uit een aantal tokens welke worden afgesloten met een $-teken a la:
# qDl$f301$oDP0.DO120$s$f302$s$oL1$
# deze functie retourneert de losse tokens van de formule in een list    
# in een formule wordt de variabele naam soms vooraf gegaan door een 'o', deze halen we er af
    
    tokens = formula.split('$')

    objrefs = []
    for token in tokens:
        # print( 'token:  ', token)
        try:
            if token[0]== 'o':
                objrefs.append(token[1:])
        except:
            jantje=1
            #nogo

    return objrefs
    

def ReadLengthBytes(myfile, pos, lengthbytes):
# CCH leest een stuk van een binary file
# lengthbytes - geeft aan hoeveel bytes er zijn vastgelegd voor de lengte van de te lezen string 

    #lengte in de eerste <lengthbytes> bytes
    if lengthbytes ==1:
        ObjectLength = myfile[pos]
        if debug:
            print(  'pos:',pos, 'obj length: ', ObjectLength)

    else:
        bt_ObjectLength = myfile[pos:pos + lengthbytes-1][::-1]
        ObjectLength = int(bt_ObjectLength.hex(), 16)

    return ObjectLength
    

def ReadObject(myfile, pos, lengthbytes, skipbytes, lengthcorrection, debug):
# CCH leest een stuk van een binary file
# lengthbytes - geeft aan hoeveel bytes er zijn vastgelegd voor de lengte van de te lezen string 
# skipbytes - correctie voor niet verklaarde verspringing van de data
    
    #lengte in de eerste <lengthbytes> bytes
    if lengthbytes ==1:
        ObjectLength = myfile[pos]
        if debug:
            print(  'pos:',pos, 'obj length: ', ObjectLength)

    else:
        bt_ObjectLength = myfile[pos:pos + lengthbytes-1][::-1]
        ObjectLength = int(bt_ObjectLength.hex(), 16)
        if debug:
            print(  'pos:',pos, 'obj length (bytes): ',bt_ObjectLength, ObjectLength)

    #Het object zelf in de volgende <lengte> bytes

    bt_ObjectContent = myfile[pos + lengthbytes:pos + lengthbytes + skipbytes * ObjectLength + lengthcorrection][::skipbytes]
    # print(bt_varname)
    ObjectContent = bt_ObjectContent.decode(coding)
    # print(ObjectLength, ObjectContent)
    
    return ObjectLength, ObjectContent

def getDPUnivMappingFromProps(widfile):
    
    widfilearchive = zf.ZipFile(widfile, mode='r')

    for info in widfilearchive.infolist():
    
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename.find(CNdocpropsfilename)>0:
            # print(info.filename)
            dpfile = widfilearchive.read(info.filename)

            # if widfile.find('PPM005')>0:
            #     for i in range(len(dpfile)):
            #         print(str(i) +  '\t' + hex(dpfile[i]) + '\t' + str(dpfile[i]) + '\t')
   
            bt_len_xml= dpfile[CNdocpropsstartposlen:CNdocpropsstartposlen+3][::-1]
            len_xml = int(bt_len_xml.hex(), 16)
            endloc_xml = len_xml + CNdocpropsstartposlen -1

            propsXMLBin = dpfile[CNdocpropsstartlocxml:endloc_xml]
            propsXML = propsXMLBin.decode(coding)

            # print(propsXML[len(propsXML)-120::])
            # print(propsXML[8::])
            xmldoc = ET.ElementTree(ET.fromstring(propsXML))

            root = xmldoc.getroot()

            for elem in root.findall('.//DOCUMENTPROPERTY'):
                if elem.attrib['NAME'] ==  'QP_SELECTED_PERSPECTIVES':
                    mappings = elem.text
                    # print(mappings)
                    dptuples = mappings.split(';')
                    DPUnivDict = {}
                    for dptuple in dptuples:
                        # print(dptuple)
                        isloc = dptuple.find('=')
                        dpcode = dptuple[0:isloc]
                        dpuniv = dptuple[isloc+1::]
                        # print(dpcode, dpuniv)
                        DPUnivDict[dpcode] = dpuniv
                        
    widfilearchive.close()                    
    
    return DPUnivDict
            
def GetDataProviderXML(varfile):

    startpos = 8
    mylen, dp_name = ReadObject(varfile, startpos, 4, 2, 0, 0)
    # print( 'Dataprovider:',mylen, dp_name)

    startpos = startpos + mylen * 2 + 4
    
    #skip some unidentified blocks
    startpos = ReadDPFileUnidentifiedBlocks(varfile, startpos)

    mylen, dp_xml = ReadObject(varfile, startpos, 4, 2, 0, 0)

    return dp_xml



def getDataProviderVarsFromXML(dpfile):
    dpxml = GetDataProviderXML(dpfile)
    # print(dpxml)
    xmldoc = ET.ElementTree(ET.fromstring(dpxml))

    # print(dpxml)

        
    varlist = []
    root = xmldoc.getroot()

    # for elem in root.findall('.//queryspec:QuerySpec'):
    # print(elem.attrib['dataProviderId'])
    # dpName = root.attrib['name'] 
    # try:
    #     dpID = root.attrib['dataProviderId'] 
    #     print(dpID)
    # except:
    #     print(dpxml)
    
    # print('Selection')
    for elem in root.findall('.//resultObjects'):
        # print(elem.attrib['identifier'], elem.attrib['name'])
        varlist.append((elem.attrib['identifier'], elem.attrib['name'],  'select'))
        
    # print( 'Condition')
    for elem in root.findall('.//condition'):
        # print(elem.attrib['itemIdentifier'], elem.attrib['itemName'])
        varlist.append((elem.attrib['itemIdentifier'], elem.attrib['itemName'],  'filter'))

    return varlist

# ------------------------------------------------------------------------------------VAR FILE PARSERS


def ReadDPObject(myfile, pos):
# CCH leest query folder of object, afhankelijk van laatste bytes
# inleeslengte altijd 4
    desclengthbytes = 4
    bt_objLen = myfile[pos:pos + desclengthbytes -1][::-1]
    objLen = int(bt_objLen.hex(), 16)

    #Het object zelf in de volgende <lengte> bytes

    bt_objText = myfile[pos + desclengthbytes:pos + desclengthbytes + objLen * 2]
    # print(bt_varname)
    objText = bt_objText.decode(coding)
    # print(ObjectLength, ObjectContent)

    startpos = pos + desclengthbytes + objLen * 2 + 4
    print(startpos)

    objTypeID = int(myfile[startpos])
    if objTypeID == 255:
        objTypeDesc =  'folder'
    else:
        objTypeDesc =  'object'

    # print( 'obj type desc:',objTypeDesc)
    
    return objLen, objText, objTypeDesc


def ReadDPObjectTree(myfile, pos, univpath, verbose):
# CCH leest query folder of object, afhankelijk van laatste bytes
# inleeslengte altijd 4
    desclengthbytes = 4
    bt_objLen = myfile[pos:pos + desclengthbytes -1][::-1]
    objLen = int(bt_objLen.hex(), 16)

    #Het object zelf in de volgende <lengte> bytes

    bt_objText = myfile[pos + desclengthbytes:pos + desclengthbytes + objLen * 2]
    # print(bt_varname)
    objText = bt_objText.decode(coding)
    # print(ObjectLength, ObjectContent)

    if verbose:
        print( 'startpos voor:',pos)
    startpos = pos + desclengthbytes + objLen * 2 + 1
    if verbose:
        print( 'typeid start:',startpos)

    objTypeID = int(myfile[startpos])
    # print( 'typeid:', objTypeID, startpos)
    if objTypeID == 64:
        objTypeDesc =  'folder'
    elif objTypeID == 8:
        objTypeDesc =  'object'
    elif objTypeID == 16:
        objTypeDesc =  'detail'        
    elif objTypeID == 32:
        objTypeDesc =  'measure'   
    else:
        objTypeDesc =  'tja...'
        
    startpos = startpos + 3
    # print( objTypeDesc, ':', objText)

    if objTypeDesc != 'folder':
        # startpos = startpos + 4
        # print(startpos)

        mylen, objID = ReadObject(myfile, startpos, 1, 1, -1, 0)
        if verbose:
            print( startpos, univpath, objText)

        startpos = startpos + mylen + 11
        if verbose:
            print( 'object:',startpos)
        checkCnt = int(myfile[startpos])
        if checkCnt==1:
            startpos = startpos + 2
            
        # any object-details to read with this object
        if objTypeDesc == 'object':
            bt_objLen = myfile[startpos:startpos + desclengthbytes -1][::-1]
            cntDetails = int(bt_objLen.hex(), 16)
            
            startpos = startpos + 4
            
            for idDet in range(cntDetails):
                # print('recursion start at:',startpos)
                startpos = ReadDPObjectTree(myfile, startpos, univpath, verbose)      
        else:
            startpos = startpos + 4

    if objTypeDesc == 'folder':
        univpath = univpath  + objText + '\\'
        if verbose:
            print( univpath)
        startpos = startpos + 13

        # als deze op 1 staat verder 2 extra optellen bij de startpos
        # als deze op 2 staat dit lezen als object aantal
        
        checkFlag = int(myfile[startpos])
        
        if verbose:
            print( 'check flag:', checkFlag)
            
        if checkFlag==0:
            startpos = startpos - 2
        
        # startpos = startpos + 13
    
        cntItems = int(myfile[startpos])
        if verbose:
            print( 'itemcount:', startpos, cntItems)
    
        startpos = startpos + 4
        # startpos = startpos + 6
        skipbytes = ReadLengthBytes(myfile, startpos, 4)
        if verbose:
            print('skipbytes:',startpos, skipbytes)
        startpos = startpos + skipbytes
        
        # print('items 2d:',cntItems)
        for f2 in range(cntItems):
            if verbose:
                print('recursion start at:',startpos)
            startpos = ReadDPObjectTree(myfile, startpos, univpath, verbose)

    return startpos
    
               
def ParseDataProviderFile(varfile, varlist, verbose):
# Todo: object type toevoegen (dim/measure/detail)
    
    startpos = 8
    mylen, dpName = ReadObject(varfile, startpos, 4, 2, 0, 0)
    # print()
    # print( 'Dataprovider:',mylen, dpName)

    startpos = startpos + mylen * 2 + 4
    
    #skip some unidentified blocks
    startpos = ReadDPFileUnidentifiedBlocks(varfile, startpos)

    mylen, dp_xml = ReadObject(varfile, startpos, 4, 2, 0, 0)

    startpos = startpos + 2 * mylen
    # print(startpos)

    startpos = startpos + 4
    mylen, paramserver = ReadObject(varfile, startpos, 4, 1, -1, 0)
    # print(startpos, mylen, paramserver)

    #Aantal objecten in de select
    
    startpos = startpos + mylen + 4
    startpos = startpos + 4 # undefined bytes
    
    bt_cnt_objects= varfile[startpos:startpos+2][::-1]
    cnt_objects = int(bt_cnt_objects.hex(), 16)
    # print( 'aantal objecten:', cnt_objects)

    blockstart = startpos + 2

    tempvarlist = []
    
    for i in range(cnt_objects):
        startpos = blockstart
        # print(startpos)
    
        bt_len_objblock= varfile[startpos:startpos+3][::-1]
        len_varblock = int(bt_len_objblock.hex(), 16)
        blockstart = len_varblock + startpos
        # print( 'volgende blok:', blockstart)
    
        startpos = startpos + 72 # 2x nolocale plus nog wat
        mylen, objName = ReadObject(varfile, startpos, 4, 2, 0, 0)
        # print(startpos, mylen,var01)
    
        startpos = startpos + 2 * mylen + 29
        mylen, objID = ReadObject(varfile, startpos, 4, 2, 0, 0)
        # print(startpos, mylen, IDvar01)

        # CCH 20240515 Hier lijkt de DPID het mees consistent aanwezig te zijn
        # alternatieven was de XML in _PROPERTIES file, maar daar is tag DPID niet altijd aanwezig
        
        DPID = objID[0:objID.find('.')]

        tempvarlist.append((DPID, objID, objName, 'DataProvider Object'))
    
        startpos = startpos + 2 * mylen + 72
        mylen, objDesc = ReadObject(varfile, startpos, 4, 2, 0, 0)
        # print(startpos, mylen, objDesc)

        # na de description op zoek naar het object universe ID:
        
        # startpos = 4209 of 6585
        # print( 'description start:',startpos)
        startpos = startpos + 2 * mylen + 17 # 3 * 4 bytes onbekend

        # print( 'locale 01')
        mylen, objLocale = ReadObject(varfile, startpos, 4, 2, 0, 0)
        
        # print( 'locale 02 BLOCK')
        startpos = startpos + 2 * mylen + 4 * 7
        mylen, objLocale = ReadObject(varfile, startpos, 4, 2, 0, 0)

        # onduidelijk stuk vd file
        startpos = startpos + mylen + 20 * 4   # CCH 20240508 onduidelijk wat er in deze 20x4 bytes zit...

        mylen, objUnivID = ReadObject(varfile, startpos, 4, 1, 0, 0)

        # print(startpos, blockstart)
        
        if verbose:
            print(objID + ';' + objName + ';' + dpName + ';' + objDesc )


    # startpos = blockstart
    # CCH 20240513 Vrij veel analyse gedaan om er achter te komen wanneer de universenaam verschijnt in de DP file
    # Eerst wordt op een recursieve manier folders en objecten gelezen uit de  'select' van de datapovider
    
    # print()    

    # print( 'init startpos: ',startpos, blockstart)
    # bij team app univ -16, bij PPM niet :-)
    startpos = blockstart + 4

    cntRootFolders = ReadLengthBytes(varfile, startpos, 4)

    if verbose:
        print( 'cntRootFolders:', cntRootFolders, startpos)
        
    startpos = startpos + 4

    for rootnr in range(cntRootFolders):
        # begin root block
        startpos = ReadDPObjectTree(varfile, startpos, '\\', verbose)
        # print( objType, ':' , objName)
    
    # print(startpos)
    mylen, quniv = ReadObject(varfile, startpos, 4, 2, 0, 0)

    
    # print(mylen, quniv)
    univName = quniv[len(dpName) + 3::]
    if verbose:
        print( 'universe name:' , univName[0:100])
   
    for obj in tempvarlist:
        # varlist.append((obj[1], obj[2], obj[3], obj[0], dpName, univName))
        varlist.append((obj[1], obj[2], obj[3], obj[0], dpName, univName))


                       
def ParseVariableFile(varfile, varlist, verbose):
# Leest het bestand met document variabelen
    
    bt_filelength = varfile[0:5][::-1]
    bt_cntvar = varfile[12:15][::-1]
    # print(bt_cntvar)
    # print(filelength)
    
    filelen = int(bt_filelength.hex(), 16)
    cntvars = int(bt_cntvar.hex(), 16)

    if verbose:
        print( 'file lengte:', filelen)
        print( 'aantal variabelen:', cntvars)
        print( 'varID; varName;varType;varParent;varUsedVar')
    
    startvarblock = 16
    len_varblock = 0

    for varnr in range(cntvars):
        
        startvarblock = startvarblock + len_varblock
        # print(startvarblock)
        
        bt_len_varblock= varfile[startvarblock:startvarblock+3][::-1]
        len_varblock = int(bt_len_varblock.hex(), 16)
        
        # print(len_varblock)
        # print( '--------------', varnr, startvarblock)
        
        mypos = startvarblock + 8
        
        #lees var naam
        mylen, varname = ReadObject(varfile, mypos, 4, 2, 0, 0)
        # print(len, varname)
        
        #lees var definitie
        mypos = mypos + 2 * mylen + 4
        mylen, varformula = ReadObject(varfile, mypos, 4, 2, 0, 0)
        # print(len, varformula)
        
        #lees var type
        mypos = mypos + 2 * mylen + 4
        vartypeid = varfile[mypos]
    
        #lees var ID
        mypos = mypos + 2
        mylen, varID = ReadObject(varfile, mypos, 1, 1, -1, 0)
        # print(varID)

                
        # igv detail parent ophalen
        mypos = mypos + mylen + 1
        varParent = ''
        if vartypeid==2:
            mylen, varParent = ReadObject(varfile, mypos, 1, 1, -1, 0)
            # print(len, varParent)

        varlist.append((varID, varname,  'Report Variable', '<>', vartypes[vartypeid],   '<>'))
        
        if verbose:
            print(varID + CNseperator + varname + CNseperator + vartypes[vartypeid] + CNseperator + varParent)



def ParseSyncVarFile(varfile, varlist, verbose):
# leest de variabelen uit de synced variable file
    
    bt_filelength = varfile[0:3][::-1]
    bt_cntvar = varfile[8:11][::-1]
    # print(bt_cntvar)
    # print(filelength)
    
    filelen = int(bt_filelength.hex(), 16)
    cntvars = int(bt_cntvar.hex(), 16)

    if verbose:
        print( 'file lengte:', filelen)
        print( 'aantal variabelen:', cntvars)

    startvarblock = 12
    len_varblock = 0
    mypos = startvarblock
    
    for varnr in range(cntvars):
        
        bt_len_varblock= varfile[mypos:mypos+3][::-1]
        len_varblock = int(bt_len_varblock.hex(), 16)
        
        # print(len_varblock)
        # print( '--------------', varnr, startvarblock)
        
        #lees var ID
        mylen, varID = ReadObject(varfile, mypos, 1, 1, -1, 0)
        # print(mylen, varID)

        
        mypos = mypos + mylen + 1
        
        #lees var naam
        mylen, varname = ReadObject(varfile, mypos, 4, 2, 0, 0)
     
        #lees omschrijving
        mypos = mypos + 2 * mylen + 4
        mylen, vardesc = ReadObject(varfile, mypos, 4, 2, 0, 0)
        # print(mylen, vardesc)

        varlist.append((varID, varname, 'Samengevoegde dimensie',  '<>', 'Dimension',  '<>'))
        
        #lees DSO
        mypos = mypos + 2 * mylen + 4 + 4
        # print(mypos)
        mylen, varDSO = ReadObject(varfile, mypos, 4, 2, 0, 0)
        # print(mylen, varDSO)
    
        #lees block, dit is een vast block, geen variabele lengte
        mypos = mypos + 2 * mylen + 4

        #lees # gecombineerder vars
        mypos = mypos + 6
        # print(mypos)
    
        bt_len_cnt_vars= varfile[mypos:mypos+3][::-1]
        cnt_vars = int(bt_len_cnt_vars.hex(), 16)
        # print( 'aantal vars:',cnt_vars)
    
        mypos = mypos + 4
        
        for combvar in range(cnt_vars):
            mylen, cvar = ReadObject(varfile, mypos, 1, 1, -1, 0)
            # print(mylen, cvar)
            mypos = mypos + mylen + 1

            if verbose:
                print( varID + CNseperator + varname + CNseperator + cvar)    



def getAllReportVariables(mywidfile):
# verzamelt alle variabelen die gebruikt worden in een rapport
    verbose = 0
    allvarlist= []
    
    # Ga alle Dataprovider files in de wid (zip) file langs
    widfilearchive = zf.ZipFile(mywidfile, mode='r')
    
    for info in widfilearchive.infolist():
    
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename[-11:] ==  '/' + CNdpvarfilename:

            dpfile = widfilearchive.read(info.filename)
            ParseDataProviderFile(dpfile, allvarlist, verbose)

    # Ga alle document variabelen bij langs
    varfile = widfilearchive.read(CNdocvarfilename)
    ParseVariableFile(varfile, allvarlist, verbose)

    # Ga alle samengevoegde (synced) dimensies bij langs
    syncfile = widfilearchive.read(CNsyncvarfilename)
    ParseSyncVarFile(syncfile, allvarlist, verbose)

    print( 'closing the file')
    widfilearchive.close()
    
    return allvarlist

# ------------------------------------------------------------------------------------VAR DEPENDENCIES


def GetReportVarDependencies(varfile, vardeps):
# Verzamel alle variabele afhankelijkheden uit formules van variabelen.
# Input: variabele file
# Output: vardeps, lijst van (VARID, DEP VARID, RELATIE TYPE)
    
    bt_filelength = varfile[0:5][::-1]
    bt_cntvar = varfile[12:15][::-1]
    
    filelen = int(bt_filelength.hex(), 16)
    cntvars = int(bt_cntvar.hex(), 16)

    startvarblock = 16
    len_varblock = 0

    for varnr in range(cntvars):
        
        startvarblock = startvarblock + len_varblock
        
        bt_len_varblock= varfile[startvarblock:startvarblock+3][::-1]
        len_varblock = int(bt_len_varblock.hex(), 16)
        
        mypos = startvarblock + 8
        
        #lees var naam
        mylen, varname = ReadObject(varfile, mypos, 4, 2, 0, 0)
        
        #lees var definitie
        mypos = mypos + 2 * mylen + 4
        mylen, varformula = ReadObject(varfile, mypos, 4, 2, 0, 0)
        
        #lees var type
        mypos = mypos + 2 * mylen + 4
        vartypeid = varfile[mypos]
    
        #lees var ID
        mypos = mypos + 2
        mylen, varID = ReadObject(varfile, mypos, 1, 1, -1, 0)
        
        # igv detail parent ophalen
        mypos = mypos + mylen + 1
        varParent = ''
        if vartypeid==2:
            mylen, varParent = ReadObject(varfile, mypos, 1, 1, -1, 0)
            vardeps.append( (varID, varParent,  'detail'))

        refobjs = ParseFormula(varformula)
        for refobj in refobjs:
            vardeps.append( (varID, refobj,  'formula'))



def GetSyncVarDependencies(varfile, vardeps):
# bepaalt welke dimensies zijn gesynchroniseerd in een synced dimension
# Input: sync variabele file
# Output: vardeps, lijst van (VARID, DEP VARID, RELATIE TYPE (= synced var))
 
    bt_filelength = varfile[0:3][::-1]
    bt_cntvar = varfile[8:11][::-1]

    filelen = int(bt_filelength.hex(), 16)
    cntvars = int(bt_cntvar.hex(), 16)

    startvarblock = 12
    len_varblock = 0
    mypos = startvarblock
    
    for varnr in range(cntvars):
        
        bt_len_varblock= varfile[mypos:mypos+3][::-1]
        len_varblock = int(bt_len_varblock.hex(), 16)

        #lees var ID
        mylen, varID = ReadObject(varfile, mypos, 1, 1, -1, 0)
      
        mypos = mypos + mylen + 1
        
        #lees var naam
        mylen, varname = ReadObject(varfile, mypos, 4, 2, 0, 0)

        #lees omschrijving
        mypos = mypos + 2 * mylen + 4
        mylen, vardesc = ReadObject(varfile, mypos, 4, 2, 0, 0)
    
        #lees DSO
        mypos = mypos + 2 * mylen + 4 + 4
        mylen, varDSO = ReadObject(varfile, mypos, 4, 2, 0, 0)
    
        #lees block, dit is een vast block, geen variabele lengte
        mypos = mypos + 2 * mylen + 4

        #lees # gecombineerder vars
        mypos = mypos + 6

        bt_len_cnt_vars= varfile[mypos:mypos+3][::-1]
        cnt_vars = int(bt_len_cnt_vars.hex(), 16)

        mypos = mypos + 4
        
        for combvar in range(cnt_vars):
            mylen, cvar = ReadObject(varfile, mypos, 1, 1, -1, 0)
            mypos = mypos + mylen + 1

            vardeps.append( (varID, cvar,  'synced var') ) 


# CNnovarcharacter = '[^A-Z^a-z^0-9].'
# CNnovarcharacter_or_end = '([^A-Z^a-z^0-9]|$)'

# CCH 20240529 gebruik onduidelijk: allleen aangeroepen in oude functie
def VarHasReportDependency(varID, docspecstring):
# hit/no hit van een VARID in een rapport XML definitie
    hit = 0
    for startchar in CNValidStartChars:

        if startchar == '"':
            varsearch = startchar + varID + '"'
            if docspecstring.find(varsearch)>-1:
                hit = 1

        if startchar == 'o':
            for endchar in CNValidEndChars:
                varsearch = startchar + varID + endchar
                if docspecstring.find(varsearch)>-1:
                    hit = 1

    return hit

def GetAllDirectReportDependencies(mywidfile, varfilelist, vardeps):

    xmldoc = getReportXML(mywidfile)
    xpaths = getReportXPaths(xmldoc)

    directvars  = []
    # get all direct dependencies
    for varpath in xpaths:
        for var in varpath[4]:
            directvars.append(var)

    # maak unieke set met varIDs in het rapport
    directvars = list(set(directvars))

    for varTuple in varfilelist:
        varID = varTuple[0]

        if varID in directvars:
            vardeps.append( ( 'report',  varID, 'report') )



def getVarDependency(VarID, vardeps, pathstring, depth=1):
# recursieve functie om alle afhankdelijkheden van een variabele in kaart te brengen.
# dus: report heeft geen dependents. Willekeurig dataprovider object kan er best veel hebben
# input: lijst met alle var dependencies (VARID, DEPVARID, DEPTYPE)
# output: lijst met dependencies voor VarID)
    
    myvardep = []
    founddep = 0
    
    for vardep in vardeps:

        if VarID == vardep[1]:

            founddep = 1
            childvardeps = getVarDependency(vardep[0], vardeps, pathstring + ' -> ' + vardep[0] , depth+1)

            if len(childvardeps) > 0:
                for tuple in childvardeps:
                    myvardep.append(tuple)
            else:
                myvardep.append( (vardep[1], vardep[2], pathstring + ' -> ' + vardep[0]  , vardep[0], depth))
         
    return myvardep



def etree_iter_path(node, tag=None, path='.', freezepath = 0):
    if tag == "*":
        tag = None
    if tag is None or node.tag == tag:
        yield node, path

    # In sommige gevallen de naam van de tag tonen
    # zodat het duidelijk is waar in het rapport de afhankdelijkheid zit
    tag_name =  ''
    if node.tag in ShowTagNameList:
        try:
            tag_name = json.loads(node[0].find(".//PVAL[@NAME='name']").text)['l']
        except:
            tag_name = ''
    if len(tag_name) > 0:
        path = path + ' (' + tag_name + ')'
            
    for child in node:

        fp = freezepath
        if node.tag in FreezePathList:
            fp = 1

        child_path = path
        if fp==0:
            child_path = path + '/' + child.tag

        for child, child_path2 in etree_iter_path(child, tag, path = child_path, freezepath = fp):
            yield child, child_path2


def traverseEltree(varID, elem, xpath, depth, foundpaths, verbose):

    fps = []
    
    if verbose:
        print(depth *  '-- ', depth, elem.tag)
    
    if re.match(r'.*o' + re.escape(varID) + CNnovarcharacter_or_end, str(elem.text)):
        fps.append(xpath)
    
    for att in elem.attrib:
        if re.match(r'.*' + re.escape(varID) + CNnovarcharacter_or_end, elem.attrib[att]):
            fps.append(xpath +  '\\' + elem.attrib[att])

    for child in elem:
        fps_child = traverseEltree(varID, child, xpath + '\\' + elem.tag, depth+1, foundpaths, verbose)
        if len(fps_child)>0:
            for fpsc in fps_child:
                fps.append(fpsc)

    return fps
            

def getVariableReportUsageOUD(varID, xmldoc):
# Deze functie controleert of een variabele in de document specificatie voorkomt.
# zo ja, dan wordt een set met XML xpaden geretourneerd
# CCH routine is nu behoorlijk traag, ooit nakijken voor optimalisatie opties
# CCH 20240429 optimalisatie gereed, zie getReportXPaths :-) 
# loopt  <1 seconde en hit met PPM026 op hetzelfde als bij deze functie!
    
    mydeps = []

    for elem, path in etree_iter_path(xmldoc.getroot()):

        # doorzoek de element teksten (zoals in rapport/tabel gebruik van variabelen)
        # NB hit op o plus varID
        if re.match(r'.*o' + re.escape(varID) + CNnovarcharacter_or_end, str(elem.text)):
            mydeps.append(path)
            
        # doorzoek de attribuut waarden (zoals bij alerters)
        # NB hit al dan niet met voorloop  'o' plus varID
        for att in elem.attrib:

            if re.match(r'.*' + re.escape(varID) + CNnovarcharacter_or_end, elem.attrib[att]):
                # mydeps.append(path +  ' [' + elem.attrib[att] +']')
                mydeps.append(path)
                
            # Besturingselementen zijn onafhankelijke XML elementen in een inputform
            if elem.attrib[att] == 'inputform' and len(elem.text)>10:
                formxml = ET.ElementTree(ET.fromstring(elem.text))

                for form_el in formxml.getroot().iter():
                    for form_att in form_el.attrib:
                        if re.match(r'' + re.escape(varID) + CNnovarcharacter_or_end, form_el.attrib[form_att]):
                            mydeps.append(path +  ' Besturingselement ')
                            # mydeps.append(path +  ' Besturingselement ' + ' [' + form_el.attrib[form_att] +']')

    return list(set(mydeps))

def getVarsFromFormula(formula):
# input: formula string
# output: all used variabels in the formula

    myvars = []
    formula_tokens = formula.split('$')
    for token in formula_tokens:
        if token[0]== 'o':
            myvars.append(token[1:])

    return(myvars)
            
    
def getReportXPaths(xmldoc):
# CCH 20240429 Haal alle xpaden op van een docspec.
# Bepaal per xpath welke variable gebruikt worden
    
    xPaths = []

    for elem, path in etree_iter_path(xmldoc.getroot()):

        # doorzoek de element teksten (zoals in rapport/tabel gebruik van variabelen)
        # NB hit op o plus varID
        if elem.tag =='ID':
            xPaths.append((path, elem.tag , elem.text, 'tag', [elem.text]))

        if elem.tag == 'ALIAS':
            xPaths.append((path, elem.tag , elem.text, 'tag', [elem.text[1:]]))
            
        if elem.tag == 'AXIS_EXPR':
            formula = elem.text
            formvars = getVarsFromFormula(formula)
            if len(formvars)>0:
                xPaths.append((path, elem.tag  , formula, 'pval tag', formvars))
        
        if (elem.tag =='PVAL' and elem.attrib['NAME']=='content'):
            tag_name = json.loads(elem.text)
            if tag_name['type']== 'formula':
                formula = tag_name['str']
                formvars = getVarsFromFormula(formula)
                if len(formvars)>0:
                    xPaths.append((path, elem.tag  ,formula , 'pval tag', formvars))
            
        if elem.tag == 'PLUGINFO':
        # CCH 20240429 some CDATA in here, so give it a special treatment
            xmlplugin = ET.ElementTree(ET.fromstring(elem.text))
            for elemplugin in xmlplugin.iter():
                
                if elemplugin.tag == 'property':
                    # print(elemplugin)    
                    if elemplugin.attrib['key']== 'formula':
                        # print(elemplugin.attrib['value'])
                        formula = elemplugin.attrib['value']
                        formvars = getVarsFromFormula(formula)
                        if len(formvars)>0:
                            xPaths.append((path, elem.tag  ,formula , 'pval tag', formvars)) 
            
        # doorzoek de attribuut waarden (zoals bij alerters)
        # NB hit al dan niet met voorloop  'o' plus varID
        for att in elem.attrib:

            if att in ( 'KEY'):
                xPaths.append((path, elem.tag + ': ' + att, elem.attrib[att], 'att', [elem.attrib[att]]))
            # if re.match(r'.*' + re.escape(varID) + CNnovarcharacter_or_end, elem.attrib[att]):
            #     # mydeps.append(path +  ' [' + elem.attrib[att] +']')
            #     mydeps.append(path)

            if att in ( 'EXPR') and elem.tag == 'EMPT_COND':
                formula = elem.attrib[att]
                formvars = getVarsFromFormula(formula)
                if len(formvars)>0:
                    xPaths.append((path, elem.tag  ,formula , 'pval tag', formvars))               
                    
            if att in ( 'EXPRESSION'):
                formula = elem.attrib[att]
                formvars = getVarsFromFormula(formula)
                if len(formvars)>0:
                    xPaths.append((path, elem.tag  , formula, 'pval tag', formvars))               
                    
            # Besturingselementen zijn onafhankelijke XML elementen in een inputform
            if elem.attrib[att] == 'inputform' and len(elem.text)>10:
                formxml = ET.ElementTree(ET.fromstring(elem.text))

                for form_el in formxml.getroot().iter():
                    for i, form_att in enumerate(form_el.attrib):
                        # print('i', i, path, att, elem.attrib[att], form_att)
                        # if re.match(r'' + re.escape(varID) + CNnovarcharacter_or_end, form_el.attrib[form_att]):
                            # mydeps.append(path +  ' Besturingselement ')
                        if form_att in ( 'ID',  'BINDOBJECT'):
                            xPaths.append((path + ' Besturingselement ', form_att, form_el.attrib[form_att], 'form_att', [form_el.attrib[form_att]]))
                            # mydeps.append(path +  ' Besturingselement ' + ' [' + form_el.attrib[form_att] +']')
    
    # return list(set(xPaths))
    return xPaths



def getAllVariableDepencencies(mywidfile, allvarlist):
# Haalt uit 3 bronnen alle variabele afhankelijkheden op:
# * Direct report * Variable formula and master detail * synchronized dimensions
    
    # dit is een list van tuples (var, vardep)
    vardeps = []
    
    # directe rapport afhankelijkheden
    GetAllDirectReportDependencies(mywidfile, allvarlist, vardeps)
    # print(len(vardeps))
    # print(vardeps)

    widfilearchive = zf.ZipFile(mywidfile, mode='r')
    
    # afhankelijkheden via formules en master-detail
    varfile = widfilearchive.read(CNdocvarfilename) 
    GetReportVarDependencies(varfile, vardeps)
    # print(len(vardeps))
    # print(vardeps)
    
    # afhankelijkheden door samengevoegde dimensies
    syncfile = widfilearchive.read(CNsyncvarfilename)  
    GetSyncVarDependencies(syncfile, vardeps)
    # print(len(vardeps))
   
    # ontdubbelen (misschien onnodig?)
    vardeps = list(set(vardeps))

    widfilearchive.close()
    
    return vardeps



def getReportXML(mywidfile):

    widfilearchive = zf.ZipFile(mywidfile, mode='r') 
    
    bt_docspecfile = widfilearchive.read(CNdocspecfilename)
    widfilearchive.close()
    
    docspec = bt_docspecfile.decode(coding)
    
    # skip some initial bytes before report tag starts
    docspec = docspec[8:]
    xmldoc = ET.ElementTree(ET.fromstring(docspec))
    
    return xmldoc



def getReportXMLString(mywidfile):

    widfilearchive = zf.ZipFile(mywidfile, mode='r') 
    
    bt_docspecfile = widfilearchive.read(CNdocspecfilename)
    widfilearchive.close()
    
    docspec = bt_docspecfile.decode(coding)
    
    # skip some initial bytes before report tag starts
    docspec = docspec[8:]

    return docspec



def getReportVarsAndDependencies(allvarlist, allvardeps):
# bepaalt van alle variabelen in de allvarlist welke afhankelijkheden er zijn
# en creeert een lijst met het oa. kortste pad van var naar report
    
    vardeps = []
    
    for i, repvar in enumerate(allvarlist):

        # print(repvar[4])
        varID = repvar[0]
        myvardep = getVarDependency(varID, allvardeps, varID, 1)

        # print('myvardep:', varID, myvardep)
        
        if len(myvardep)>0:
            cnt_deps = 0

            # bepaal minimale path diepte van variabele naar report
            min_repdepth = 100
            for dep in myvardep:
                cnt_deps = cnt_deps + 1
                if dep[3] == 'report':
                    if dep[4] < min_repdepth:
                        min_repdepth = dep[4]
                    
            showsample = 1
                
            # als een pad gevonden is van variabele naar report
            if min_repdepth<100:

                if min_repdepth ==1:
                    deptype = 'Direct'
                else:
                    deptype = 'Indirect'
                
                for dep in myvardep:
                    if dep[3] == 'report':
                        if dep[4] == min_repdepth and showsample==1:
                        # als het korste pad is gevonden    
                            showsample = 0
                            vardeps.append( (str(i), repvar[0], repvar[1], repvar[2], repvar[4], repvar[3], repvar[5], deptype, dep[2], str(cnt_deps) ) )
            else:
            # variabele zonder pad naar report
                for j, dep in enumerate(myvardep): 
                    if j==0:
                        vardeps.append( (str(i), repvar[0], repvar[1], repvar[2], repvar[4], repvar[3], repvar[5],'Nee', dep[2], str(cnt_deps) ))                    
        else:
            vardeps.append( (str(i), repvar[0], repvar[1], repvar[2],  repvar[4], repvar[3], repvar[5], 'Nee','' ,'0') )

    return vardeps
    
def getAlerterStatus(myWebiPathReport):
# bepaalt van alle alerters in de alert lijst van het report of deze wel worden gebruikt
    
    # get all alerters
    alerterlist = []

    xmldoc = getReportXML(myWebiPathReport)
    for elem in xmldoc.find( './/ALERTER_DICT'):
        alerterlist.append((elem.attrib['ID'], json.loads(elem[0][1].text)['l']))

    used_alerters = []
    
    for elem in xmldoc.getroot().iter():
        # print(elem.tag)
        if elem.tag == 'PVAL':
            # print(str(elem.attrib))
            
            for att in elem.attrib:
                if att== 'NAME' and elem.attrib[att] ==  'alerters':
                    # print(elem.attrib[att])
                    # print(elem.text)
                    for alid in str(elem.text).split(';'):
                        used_alerters.append(alid)
    
    usedalerterlist = list(set(used_alerters))
    usedalerterlist.sort()
    
    alerterstatuslist = []
    
    for alerter in alerterlist:
        status =  'not used'
        if alerter[0] in usedalerterlist:
            status =  'used'
        alerterstatuslist.append((alerter[0], alerter[1], status))

    return alerterstatuslist


def getReportVarsXPathsOUD(myWebiPathReport, allvarlist):
# CCH 20240429 deze werkt goed, dus voor de zekerheid nog even bewaren

    # Get report dependencies plus xpath hint
    xmldocstring = getReportXMLString(myWebiPathReport)
    xmldoc = getReportXML(myWebiPathReport)
    varreportusage = []
    
    for i, myvar in enumerate(allvarlist):
        varID = myvar[0]

        print(i, varID)
        #first a quick search
        varrepusage = []
        if VarHasReportDependency(varID, xmldocstring):
            varrepusage = getVariableReportUsageOUD(varID, xmldoc)
    
        for xpath in varrepusage:
            varreportusage.append((myvar[0], myvar[1], myvar[2], myvar[3], xpath))
    
    # df_varreportusage = pd.DataFrame(varreportusage, columns =['Var ID', 'Name', 'Type', 'Info', 'Report XPath'])

    return varreportusage


 

