# RELEASES

# CCH 202508228
#
# * Report naam lookup dictionary verwijderd. IPV property 'name' wordt property 'nameinrepo' gebruikt, die is veel vaker actueel
#   Voor indivuele rapport analyse is het sws duidelijk om welk rapport het gaat
# * Meer robuustheid ingebouwd, zodat falen van de rapport analyse minder voor komt. 

# CCH 20250710
# * Upgrade functie ParseDataProviderFile, zodat query's met door BO ingevoegde hulpobjecten in de SQL ook 
#   correct worden verwerkt. Dit zit bv. in de BALxxx-rapporten

# CCH 20250506
# * Groep-variabelen ingevoegd uit aparte file in de WID. Soms staat de var al bij de rapportvariabelen -> opgelost

# CCH 20250401
# * functie ReadObject aangepast, zodat filepointer gelijk wordt verzet naar plek achter het gelezen object
# * functie ReadLengthBytes ingevoegd om eenvoudig lengte te lezen uit x aantal bytes van een file vanaf startpositie

# CCH 20250325
# * Bugfix: variabelen gebruikt in standaard waarde voor IBE filtering werden niet meegenomen bij dependency bepaling

# CCH 20250319 
# * toont sync variabelen alleen als ze ook variabelen syncen. Syncfile blijft kennelijk soms gevuld met lege sync vars, die niet zichtbaar zijn in WebI.
# * bepaalt dependencies tussen objecten in dataproviders bij gebruik in de filters

# DONE
# 01. Character decoding van properties / rapportnamen fixen (CCH 20240617)
# 05. Lijst met losse formules in een rapport XML definitie, dus zonder variabele (CCH 20240613)
# 06. Dependency dp object dat dienst als filter voor andere dp (CCH 20250319)

# --------------------------------------------------------

# BACKLOG
# 02. Dataprovider query's met complexe filtering (RANK) wordt nu geskipt
# 03. Excel dataproviders kunnen niet worden verwerkt
# 04. Controleren wat ie doet met dataproviders met een UNION er in
# 05. Een aantal rapporten komen niet door de parser en worden dus niet geanalyseerd.
# 06. Groupingvars toevoegen aan getVarNameDictionary(widfilearchive)

# TECHNICAL DEBT
# 01. Functies ingebouwd om descriptions bij query objecten te vinden. Gestrand op niet kunnen vinden van link tussen DP en DS -> code verwijderen

# --------------------------------------------------------

# INFO
# Universe object paden ophalen om duplicaten bij joinen met universe info te voorkomen
# Beschikbaar in bestand \Data\C3\Document_LocalizedObjects
#
# CCH 20250512 rapport CUID is in de repository altijd uniek. In de reportproperties wordt hij helaas niet altijd netjes bijgewerkt, waardoor duplicaten ontstaan
# bijv. bij kopie bestaand rapport. Daarom kan de rapportnaam voorlopig het beste als unieke key worden gebruikt.


import xml.etree.ElementTree as ET
from lxml import etree
import re
import zipfile as zf
import json
import glob
import pandas as pd


# Constants
CNcoding =  'latin1'
# coding of report properties 
CNcoding_alt01 = 'utf8'

CNvartypes = ( 'Measure', 'Dimension',  'Detail')

CNformulatokensDict = {'f265': 'Gemiddelde',	'f344': 'MaandenTussen',		'f302': '+',	'f475': 'Pos',
'f266': 'Aantal',	'f345': 'Macht',		'f303': '-',	'f484': 'LaatsteDagVanMaand',
'f267': 'Max',	'f346': 'Kwartaal',		'f304': '*',	'f485': 'LaatsteDagVanWeek',
'f268': 'Min',	'f347': 'RelatieveDatum',		'f305': '/',	'f487': 'Rapportfilter',
'f269': 'Percentage',	'f348': 'Vervangen',		'f308': 'Blok',	'f492': 'AantalRijen',
'f270': 'Som',	'f349': 'Rechts',		'f309': 'IsLeeg',	'f496': 'Log10',
'f272': 'Alle',	'f350': 'VerwSptRechts',		'f310': 'Lengte',	'f500': 'Opvullen',
'f273': 'Eenmalig',	'f352': 'NaarDatum',		'f312': 'SubReeks',	'f501': 'Zelf',
'f274': '<=',	'f353': 'NaarGetal',		'f315': 'Afronden',	'f503': 'Rang',
'f275': '>=',	'f354': 'Afkappen',		'f316': 'Abs',	'f504': 'Boven',
'f276': '>',	'f355': 'ReactieGebruiker',		'f317': 'Tussen',	'f506': 'Regelnummer',
'f277': '<',	'f356': 'Week',		'f318': 'AfrondenBoven',	'f519': 'GeenFilter',
'f278': '<>',	'f357': 'Jaar',		'f319': 'HuidigeGebruiker',	'f522': 'Eerste',
'f279': 'En',	'f358': 'WaarBij',		'f320': 'NaamDag',	'f523': 'Laatste',
'f280': 'Of',	'f359': 'CumulatieveSom',		'f321': 'DagVanMaand',	'f524': 'Dan',
'f281': 'Niet',	'f362': 'CumulatiefGemiddelde',		'f322': 'DagVanWeek',	'f525': 'Anders',
'f282': 'Als',	'f363': 'CumulatiefAantal',		'f324': 'DagenTussen',	'f526': 'AndersAls',
'f283': 'In',	'f366': 'Vorige',		'f328': 'AfrondenBeneden',	'f528': 'SamenvoegenForceren',
'f284': 'VoorAlles',	'f456': '(',		'f329': 'NotatieDatum',	'f533': 'RapportNaam',
'f285': 'VoorElke',	'f457': ')',		'f330': 'NotatieNummer',	'f536': 'Bloknaam',
'f286': 'HoofdGedeelte',	'f458': ';',		'f332': 'IsFout',	'f548': 'Aanwijzingsoverzicht',
'f287': 'Sectie',	'f462': 'InLijst',		'f333': 'IsNummer',	'f549': 'Rapportfilteroverzicht',
'f288': 'Rapport',	'f463': 'Gegevensbron',		'f334': 'IsReeks',	'f557': 'TijdDim',
'f291': 'NaamVan',	'f466': 'LinksOpvullen',		'f336': 'Links',	'f558': 'JaarPeriode',
'f292': 'AnalyseFilters',	'f467': 'RechtsOpvullen',		'f337': 'VerwSptLinks',	'f560': 'MaandPeriode',
'f294': 'NaamDocument',	'f468': 'KleineLetter',		'f340': 'Vergelijken',	'f561': 'WeekPeriode',
'f297': 'HuidigeDatum',	'f469': 'Hoofdletters',		'f341': 'Rest',	'f562': 'DagPeriode',
'f298': 'HuidigeTijd',	'f470': 'Hoofdletter',		'f342': 'Maand',	'f671': 'DatumsTussen',
'f299': 'DatumLaatsteUitvoering',	'f471': 'WoordHoofdletter',		'f343': 'MaandVanJaar',	
'f301': '=',	'f474': 'Teken' }

# XML key voor het zoeken naar query conditie operands die andere dataprovider objecten gebruiken
CNXMLKeyType = '{http://www.w3.org/2001/XMLSchema-instance}type'

# Bestandsnamen in het .WID zip archief
CNdpvarfilename = 'DP_Generic'
CNdocvarfilename =  'Data/C3/DocumentVariable'
CNsyncvarfilename =  'Data/C3/DATAPROVIDERS/SynchroManager'
CNrefvarfilename =  'Data/C3/Document_RefCells'
CNdocspecfilename =  'Data/RE/DOCSPEC'
CNdocpropsfilename =  '_PROPERTIES'
CNdsmanagerfilename = 'Data/C3/DATASOURCES/DSManager'
CNgroupvarfilename = 'Data/C3/DocumentGrouping_Var'

# CCH 20250828
# In de document properties staan 2 rapportnamen: name en nameinrepo
# nameinrepo is actueler dan 'name'
# deze twee kunnen verschillen als een rapportnaam in startpunt wordt aangepast, maar het rapport nog niet is geopend om aan te passen / op te slaan.
# nameinrepo is soms ook niet actueel, maar wel meer actueel.
CNpropreportname = 'nameinrepo'

# Byte positities in de doc props file
CNdocpropsstartposlen = 8
CNdocpropsstartlocxml = 116

# Formules met minimaal deze complexiteit worden gerapporteerd
CNswervingformulacomplexity = 6

# reguliere expressies voor het bepalen van de grenzen van de variabele-ID's in formules
CNnovarcharacter = '[^A-Z^a-z^0-9].'
CNnovarcharacter_or_end = '([^A-Z^a-z^0-9]|$)'

# output column seperator
CNseperator = ';'

# CCH for some XML tags we want to display it's instance name
CNShowTagNameList = { 'REPORT','ALERTER', 'VTABLE',  'SECTION', 'XELEMENT', 'XTABLE', 'CELL'}

# CCH Relevant for report statistics
CNGetStats= ['VTABLE', 'XTABLE', 'XELEMENT']

# CCH some detail XML tags are not adding usefull information about the variable location
# so the shown XML path is not extended after these tags
FreezePathList = { 'ALERTER', 'VTABLE', 'XELEMENT', 'XTABLE'}

#valid start and ending chars around var ID's in docspec XML
CNValidStartChars = { '"', 'o' }
CNValidEndChars = { '<', '"', '$', ','}

# CNDSMVarStartBlock = b'\xff\xff\xff\xffX\x02\x00'
CNDSMVarStartBlock = b'\x00\x00\x00D\x00S\x00'

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------- READING FILES ---------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def BinaryFileViewer(varfile, exportfilename, verbose):
# CCH for analysis of binary files. Dumps binary content to formatted textfile
    # f = open(myfile, mode="rb")
    # varfile = f.read()
    # f.close()

    if verbose ==0:
        flatfile = open( exportfilename, mode =  'a')
    
    for i in range(len(varfile)):

        if verbose:
            print(str(i) +  '\t' + hex(varfile[i]) + '\t' + str(varfile[i]) + '\t')
        else:
            flatfile.write(str(i) +  '\t' + hex(varfile[i]) + '\t' + str(varfile[i]) + '\t\n')

    if verbose ==0:
        flatfile.close()


def ReadDPFileUnidentifiedBlocks(dpfile, startpos):
# Functie tbv parsen van de dataprovider file
    
    startpos = startpos + 18 #Onbekende bytes
    
    # De volgende 4 bytes geeft aantal blokken van 17 bytes dat er op volgt weer.
    # Wat er in deze blokken staat is niet duidelijk
    cntblocks = ReadLengthBytes(dpfile, startpos, 3)
    # print(startpos, cntblocks)
        
    return startpos + 17 * cntblocks + 4


def ReadDocumentProps(widfilearchive):
# Leest de documentproperties die in xml formaat zijn opgeslagen in de propsfile
# Output naar dictionary property -> waarde
    DocPropDict = {}

    for info in widfilearchive.infolist():
    
        # Als er een props file gevonden is, dan uitlezen
        if info.filename.find(CNdocpropsfilename)>0:
            # print(info.filename)

            dpfile = widfilearchive.read(info.filename)

            len_xml = ReadLengthBytes(dpfile, CNdocpropsstartposlen, 3)
            endloc_xml = len_xml + CNdocpropsstartposlen -1

            propsXMLBin = dpfile[CNdocpropsstartlocxml:endloc_xml]
            propsXML = propsXMLBin.decode(CNcoding_alt01)

            xmldoc = ET.ElementTree(ET.fromstring(propsXML))

            root = xmldoc.getroot()

            for elem in root.findall('.//DOCUMENTPROPERTY'):
                dictkey = elem.attrib['NAME']
                dicttext = elem.text
                DocPropDict[dictkey] = dicttext                 

    #  Naam naar niewe code
    # DocPropDict['name'] = newReportName(DocPropDict['name'])
    
    return DocPropDict


def getDPUnivMappingFromProps(widfilearchive):
# Input: wid file archive    
# Output: dictionary die de dataprovider ID vertaalt naar (DP Naam, DP Type, universe naam)
    
    DPUnivDict = {}

    for info in widfilearchive.infolist():
    
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename.find(CNdocpropsfilename)>0:
            # print(info.filename)
            propfile = widfilearchive.read(info.filename)
            # BinaryFileViewer(propfile,'c:\\temp\\widproces\\propfile.txt',0)

            len_xml = ReadLengthBytes(propfile, CNdocpropsstartposlen, 3)
            
            endloc_xml = len_xml + CNdocpropsstartposlen -1

            lenrest = ReadLengthBytes(propfile, endloc_xml + 1, 3)
            # print(lenrest)

            startpos = endloc_xml + 17
            while (startpos<lenrest+endloc_xml):
                DPID, startpos = ReadObject(propfile, startpos, 4, 2, 0, 0)

                DPType, startpos = ReadObject(propfile, startpos, 4, 2, 0, 0)

                DPName, startpos = ReadObject(propfile, startpos, 4, 2, 0, 0)

                UnivName, startpos = ReadObject(propfile, startpos, 4, 2, 0, 0)
                startpos = startpos + 27

                # CCH 20240607 Sommige prop files hebben op deze plek soms 4 bytes extra staan.
                # We verwachten een 'D' (=code(68)) op een bepaalde plek
                # want alle dp vars beginnen met 'DP..'
                if int(propfile[startpos+8])==68:
                    startpos = startpos + 4
                    
                DPUnivDict[DPID] = (DPName, DPType, UnivName)
            
    return DPUnivDict


def GetDataProviderXML(dpfile):
# Haalt de xml tekst uit een dataprovider-file op
    
    startpos = 8
    dp_name, startpos = ReadObject(dpfile, startpos, 4, 2, 0, 0)

    #skip some unidentified blocks
    startpos = ReadDPFileUnidentifiedBlocks(dpfile, startpos)

    # als er geen '<' wordt gevonden op startpositie+4 dan kennelijk zonder skipbyte
    if dpfile[startpos+4] != 60:
        dp_xml, startpos = ReadObject(dpfile, startpos, 4, 1, -1, 0)
    else:    
        dp_xml, startpos = ReadObject(dpfile, startpos, 4, 2, 0, 0)

    return dp_xml


def ReadObject(myfile, pos, lengthbytes, skipbytes, lengthcorrection, debug):
# CCH leest een stuk van een binary file
# lengthbytes - geeft aan hoeveel bytes er zijn vastgelegd voor de lengte van de te lezen string 
# skipbytes - correctie voor niet verklaarde verspringing van de data
# lengthcorrection - als lengte correctie nodig is, grootte van deze correctie
#
# return
# ObjectContent - de gelezen string
# pos_nw - de nieuwe pointer (mypos) in de file, na de gelezen string
    
    bt_ObjectLength = 'nvt'
    #lengte in de eerste <lengthbytes> bytes
    if lengthbytes == 1:
        ObjectLength = myfile[pos]
        if debug:
            print(  'pos:',pos, 'obj length: ', ObjectLength)

    else:
        ObjectLength = ReadLengthBytes(myfile, pos, lengthbytes - 1)
            
        if debug:
            print(  'pos:',pos, 'obj length (bytes): ',bt_ObjectLength, ObjectLength)

    pos_nw = pos + skipbytes * ObjectLength + lengthbytes 
    
    bt_ObjectContent = myfile[pos + lengthbytes:pos_nw + lengthcorrection][::skipbytes]

    ObjectContent = bt_ObjectContent.decode(CNcoding)
            
    return ObjectContent, pos_nw


def ReadLengthBytes(file, pos, len):
# file: some binary file
# pos: byte position of lengt begin
# len: number of bytes containing the length
# return length: decimal number stored in len bytes of the file

    # [::-1]: read bytes in reversed order
    # in this way a proper hexadecimal number is created
    bt_length = file[pos:pos + len][::-1]    
    length = int(bt_length.hex(), 16)
    
    return length


def SearchDSMVarStart(dsmfile, startpos):
# Hulpfunctie voor het vinden van de positie in de file waar relevante info staat
# van query objecten die in het rapport een andere naam hebben gekregen.

    filesize =  ReadLengthBytes(dsmfile, 0, 4)
    filepos = startpos
    # while (filepos < filesize-30) and ( dsmfile[filepos:filepos+7] != CNDSMVarStartBlock or dsmfile[filepos+24:filepos+28] != b'\x01\x00\x02\x00') :
    while (filepos < filesize-30) and ( dsmfile[filepos:filepos+7] != CNDSMVarStartBlock) :
        filepos = filepos + 1

    return filepos


def DebugPrint(myfile, filepos, myrange):
# Hulpfunctie om een stukje van een binair bestand op het scherm te tonen
# Helpt bij het reverse engineeren van de binaire bestandsstructuren
    
    print('----------------------')
    for i in range(2 * myrange):
        pointer = filepos - myrange + i
        if pointer == filepos:
            ind = ' <----'
        else:
            ind = ''
            
        print(str(pointer) +  '\t' + hex(myfile[pointer]) + '\t' + str(myfile[pointer]) + '\t' + chr(int(myfile[pointer])) + ind)
    print('----------------------')

    
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------- VAR FILE AND FORMULA PARSING ------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def ParseGroupingVarFile(groupvarfile, varNameDict, verbose):
# CCH 20250502 Als er een GroupVar file is zijn er groeperingen gemaakt in het rapport
# Deze functie haalt de VarID's op van deze groeperingen, in de keys van een dictionary
# [1] VarID, [2] VarName, [3] VarType, [4] Var Formula, [5] Formula complexity, [6] Var description, [7] Var Info (dpname/type), [8] Universe 

# In een aantal gevallen is de groep-variabele in het rapport opgenomen als report variabele met een if-then-else statement
# Als dat zo is, staat in de groupvarfile een '&' voor de variabele ID en begint de var naam met een spatie
# In andere gevallen staat de variabele alleen in de groupvar file
    
    verbose = 0
    groupvarlist = []
    filesize =  ReadLengthBytes(groupvarfile, 0, 4)

    filepos = 8
    cntGrpVars =  ReadLengthBytes(groupvarfile, filepos, 4)

    if verbose:
        print(cntGrpVars)

    varfilepos = 12    

    for i in range(cntGrpVars):

        filepos = varfilepos
        
        grpsize =  ReadLengthBytes(groupvarfile, filepos, 4)
        filepos = filepos + 8
        
        GrpVarName, filepos = ReadObject(groupvarfile, filepos, 4, 2, 0, 0)
        GrpVarID, filepos = ReadObject(groupvarfile, filepos, 4, 2, 0, 0)
        GroupedVarID, filepos = ReadObject(groupvarfile, filepos, 4, 2, 0, 0)

        # GroupVarDict[GrpVarID] = GroupedVarID

        if GroupedVarID in varNameDict:
            GrpFormula = '< Groepeert de variabele \'' + varNameDict[GroupedVarID] + '\' (' + GroupedVarID + ') >'
        else:
            GrpFormula = '< Groepeert de variabele ['+ GroupedVarID + '] >'
        
        # CCH 20250506 grpsize matcht niet goed met de andere complexity uit formules, 
        # dus eerst maar vast op 200 gezet, zodat ie wel in het oog springt, maar niet te hoog.
        
        # groupvarlist.append([GrpVarID, GrpVarName, 'Groep Variable', GrpFormula, grpsize, 'Dimension'])
        groupvarlist.append([GrpVarID, GrpVarName, 'Groep Variable', GrpFormula, 200, 'Dimension'])
        
        if verbose:
            print(GrpVarName, GrpVarID, GroupedVarID)

        varfilepos = varfilepos + grpsize

    return groupvarlist


def ParseDSManager(dsmfile, verbose):
# input: de DataSourceManager file
# output: Dictionary van in het rapport aangepaste omschrijvingen bij query objecten
# file structuur:
# aanname: Overschreven query object description begint na blok b'\xff\xff\xff\xffX\x02\x00' en na 24 bytes dan b'\x01\x00\x02\x00
# CCH 20250423 klopt niet, ik heb er die van deze regel afwijken
    
    
    # BinaryFileViewer(dsmfile, 'c:\\temp\\td_t5.csv', 0)
    
    verbose = 0
    QueryObjectDescription = {}
    
    filesize =  ReadLengthBytes(dsmfile, 0, 4)
    if verbose:
        print('file length:', filesize)

    filepos = 0       
    # als de startstring gevonden is voor het einde van de file
    while filepos < filesize-30:

        startpos = filepos
        
        filepos = SearchDSMVarStart(dsmfile, startpos)

        if filepos < filesize-30:
            if verbose:
                print('start gevonden:', filepos, dsmfile[filepos:filepos+7])
        
            # filepos = filepos + 8
            filepos = filepos - 21
            
    
            # Nu aangekomen op het blok met DSO codes, aangepaste omschrijvingen en properties
            # begint met blocklengte, doen we nu niets mee
            blok01len =   ReadLengthBytes(dsmfile, filepos, 4)
        
            filepos = filepos + 8
            
            cntvars = ReadLengthBytes(dsmfile, filepos, 4)
            filepos = filepos + 4
        
            if verbose:    
                print('aantal vars: ', cntvars)
        
            varblockstart = filepos
            
            for varnr in range(cntvars):
                filepos = varblockstart
                varblocklen = ReadLengthBytes(dsmfile, varblockstart, 4)
                filepos = filepos + 4
                # print(varblocklen)
                
                # start van een volgende variabele
                varblockstart = varblockstart + varblocklen
                if verbose:        
                    print('vbs:', varblockstart)
                
                # naar start van VarID
                filepos = filepos + 4
                # print(filepos)
                if verbose:
                    DebugPrint(dsmfile, filepos, 10)
                # print(int(dsmfile[filepos]), int(dsmfile[filepos+1]), int(dsmfile[filepos+2]), int(dsmfile[filepos+3]))
        
                VarID, filepos = ReadObject(dsmfile, filepos, 4, 2, 0, 0)
                # print('VarID', VarID, filepos)
        
                # print(int(dsmfile[filepos]), int(dsmfile[filepos+1]), int(dsmfile[filepos+2]), int(dsmfile[filepos+3]))
                VarID, filepos = ReadObject(dsmfile, filepos, 4, 2, 0, 0)
        
                if verbose:
                    print('VarID', VarID, filepos)
        
                filepos = filepos + 32
                Locale, filepos = ReadObject(dsmfile, filepos, 4, 2, 0, 0)
                if verbose:
                    print('loc', Locale)
                filepos = filepos + 4
                Locale, filepos = ReadObject(dsmfile, filepos, 4, 2, 0, 0)
                # print('loc', Locale)
                VarDesc, filepos = ReadObject(dsmfile, filepos, 4, 2, 0, 0)
                if verbose:
                    print('VarDesc', VarDesc)
        
                QueryObjectDescription[VarID] = VarDesc

    
    return QueryObjectDescription


def ParseRefVarFile(varfile, verbose):
# parses the file containing reference variabeles
# returns list of : varID, varName, 'Referentie Variable', varRef, varDesc
    
    refvarlist = []

    filelength =  ReadLengthBytes(varfile, 0, 4)

    # refvar file might be (almost) empty when no refs are made in de report
    if filelength > 20:
        
        startpos = 8
    
        varcount = ReadLengthBytes(varfile, startpos, 2)
        # print(varcount)
    
        startpos = startpos +  4
    
        blocklength = ReadLengthBytes(varfile, startpos, 3)
        # print(blocklength)
    
        len_varblock = 0
        startvarblock = startpos
        
        for i in range(varcount):
            if verbose:
                print('---', i, '---')
    
            startvarblock = startvarblock + len_varblock
            # print(startvarblock)
            
            len_varblock = ReadLengthBytes(varfile, startvarblock, 3)
            
            if verbose:
                print('varblock:', startvarblock, len_varblock)
    
            startpos = startvarblock + 8
            
            varRef, startpos = ReadObject(varfile, startpos, 4, 2, 0, 0)
            if verbose:
                print(mylen, varRef)

            # skip some insignificant bytes
            startpos = startpos + 2
            
            varID, startpos = ReadObject(varfile, startpos, 1, 1, -1, 0)
            if verbose:
                print(mylen, varID)    

            # skip some insignificant bytes
            startpos = startpos + 44
        
            varName, startpos = ReadObject(varfile, startpos, 4, 2, 0, 0)
            if verbose:
                print(startpos, mylen, varName)

            # skip some insignificant bytes
            startpos = startpos + 77

            if startpos<filelength:
                varDesc, startpos = ReadObject(varfile, startpos, 4, 2, 0, 0)
                if verbose:
                    print(startpos, mylen, varDesc)
            else:
                varDesc = ''
                
            refvarlist.append([varID, varName, 'Referentie Variable', varRef, varDesc])

    return refvarlist


def ParseDataProviderFile(dpfile, verbose):
# returns list of : (objID, objName, 'DataProvider Object',DPID)
# Todo: object type toevoegen (dim/measure/detail)

    dpvarlist = []
    startpos = 8
    
    dpName, startpos = ReadObject(dpfile, startpos, 4, 2, 0, 0)

    if verbose:
        print(startpos, dpName)
            
    #skip some unidentified blocks
    startpos = ReadDPFileUnidentifiedBlocks(dpfile, startpos)

    if verbose:
        print('dp_xml:',startpos)

    # CCH 20240628 De DP XML wordt in verschillende formaten opgeslagen (soms met skipbyte, soms zonder)
    if verbose:
        print('testbyte:', dpfile[startpos+4])
    
    # als er geen '<' wordt gevonden op startpositie dan kennelijk zonder skipbyte
    if dpfile[startpos+4] != 60:
        dp_xml, startpos = ReadObject(dpfile, startpos, 4, 1, -1, 0)
    else:    
        dp_xml, startpos = ReadObject(dpfile, startpos, 4, 2, 0, 0)

    if verbose:
        print('paramserver:',startpos)
        DebugPrint(dpfile, startpos, 20)

    # CCH 20250827 Bij enkele bestanden staat de startpos nu 4 bytes te ver
    # Hiervoor corrigeren
    if dpfile[startpos] == 0x1 and dpfile[startpos+1] == 0x0:
        # print('nu ellendeling')
        startpos = startpos - 4
    
    # startpos = startpos + 4
    paramserver, startpos = ReadObject(dpfile, startpos, 4, 1, -1, 0)
    
    startpos = startpos + 4 # undefined bytes
      
    cnt_objects = ReadLengthBytes(dpfile, startpos, 2)

    if verbose:
        print(startpos, 'cnt obj:', cnt_objects)
    
    blockstart = startpos + 2

    for i in range(cnt_objects):
        startpos = blockstart
        
        if verbose:
            print(startpos)
    
        len_varblock = ReadLengthBytes( dpfile, startpos, 3)
        
        blockstart = len_varblock + startpos

        if verbose:
            print( 'volgende blok:', blockstart)

        # 20250710 Spring naar lengte objectnaam, vind hier dus positie van start object ID
        startpos = startpos + 8
        
        len_varnameblock = ReadLengthBytes( dpfile, startpos, 4)
        startpos_varid = startpos + len_varnameblock
        
        if verbose:
            print('sp + 8:', startpos, len_varnameblock)

        #  spring naar mogelijk locale block
        startpos = startpos + 12
        if verbose:
            print('sp', startpos)
        len_localeblock = ReadLengthBytes( dpfile, startpos, 4)

        if verbose:
            print('llb:',len_localeblock)
            
        startpos = startpos + len_localeblock * 2 + 4
        startpos += 4
        
        # nog een mogelijk locale block
        if verbose:
            print('sp2', startpos)

        len_localeblock = ReadLengthBytes( dpfile, startpos, 4)
        startpos = startpos + len_localeblock * 2 + 4

        # nu staan we op start mogelijke variabele omschrijving
        objName, startpos = ReadObject(dpfile, startpos, 4, 2, 0, 0)

        if verbose:        
            print(startpos, 'obj name:', objName)

        startpos = startpos_varid + 12
        objID, startpos = ReadObject(dpfile, startpos, 4, 2, 0, 0)
        # print(startpos, mylen, IDvar01)

        DPID = objID[0:objID.find('.')]

        # CCH 20250710 In een heel enkel geval voegt BO een hulpobject in een query in 
        # die niet als query object zichtbaar is, bv. BAL001 / universe query.
        # Dit hulpobject heeft geen naam en hoeft ook niet zichtbaar te zijn in de variabele lijst tbv afhankelijkheden
        if objName != '':
            dpvarlist.append([objID, objName, 'DataProvider Object',DPID])
        
        if verbose:
            print(objID + ';' + objName + ';' + dpName )

    return dpvarlist

                
def ParseVariableFile(varfile, verbose):
# Leest het bestand met document variabelen
# returns list of: (varID, varname,  'Report Variable',vartypes[vartypeid], '\'' + varFormulaText, varUsedRefs, varDescription)

    repvarlist = []

    filelen = ReadLengthBytes(varfile, 0, 5)

    cntvars = ReadLengthBytes(varfile, 12, 3)
    

    if verbose:
        print( 'file lengte:', filelen)
        print( 'aantal variabelen:', cntvars)
        print( 'varID; varName;varType;varParent;varUsedVar')
    
    startvarblock = 16
    len_varblock = 0

    for varnr in range(cntvars):
        
        startvarblock = startvarblock + len_varblock
        # print(startvarblock)
        
        len_varblock = ReadLengthBytes(varfile, startvarblock, 3)
        mypos = startvarblock + 8
        
        #lees var naam
        varname, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)

        #lees var definitie
        varformula, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)

        #lees var type
        vartypeid = varfile[mypos]
    
        #lees var ID
        mypos = mypos + 2
        
        varID, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)
        
        # igv detail parent ophalen
        # CCH 20250328 leuk bedacht, maar die is soms gewoon leeg in het rapport...
        
        varParent = ''

        # als de parent niet gevuld is komt gelijk de '255' byte.
        if vartypeid==2 and varfile[mypos]!=255:
            varParent, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)

            # correctie ntb
            mypos = mypos - 1

        # Dan volgt een '255' karakter: overslaan
        mypos = mypos + 1

        # CCH 20250327 een blok met data waarin de var naam nog een keer herhaald wordt, overslaan dus
        len_locblock = ReadLengthBytes(varfile, mypos, 3)
        
        mypos = mypos + 4 + len_locblock

        # dan volgen 12 bytes met onduidelijke parameter waarde
        mypos = mypos + 12
        # daarna lengte van het hele omschrijving block (4 bytes)
        # omdat we de lengte van het hele varblock al hebben opgeslagen, hebben we deze niet nodig om netjes uit te lijnen voor een volgende variabele
        mypos = mypos + 4
        # daarna 4 bytes voor parameter waarden + 4 bytes misschien voor een locale melding, maar niet anders dan 0000 gezien
        mypos = mypos + 8

        # CCH 20250328 Voorlopige conclusie: 
        # De nolocales slaan op de tekst die er na volgt.
        # Als beide nolocales ontbreken mag geconcludeerd worden dat er geen tekst volgt
        # en zelfs dat er geen lengte (0000) van de volgende tekst is opgeslagen in de file.
        # Geconstateerd dat als eenmaal en variabele description is ingevoegd, en daarna verwijderd
        # dat dan de nolocales blijven staan, en de 4 bytes voor de lengte van de tekst ook. Alleen die is dan leeg : 0000
        
        # dan 4 bytes voor 'locale' lengte

        mypos_prelocale = mypos
        
        locname, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
        
        # dan soms 4 bytes (1000) er tussen, waar we niks mee doen
        if varfile[mypos]==1:
            mypos = mypos + 4

        # dan weer 4 bytes voor 'locale'
        locname, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)

        mypos_postlocale = mypos

        # en finally, we zijn de variabele omschrijving

        # print(mypos_postlocale, mypos_prelocale)
        vardesc = ''        
        # als minstens 1 van beide locales gevuld is 
        if mypos_postlocale - mypos_prelocale > 8:
            vardesc, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)

        # print(varID, varname)
        
        varUsedRefs = getVarRefsFromFormula(varformula)
        # print(varname, varformula, varUsedRefs)
        
        repvarlist.append([varID, varname,  'Report Variable', CNvartypes[vartypeid],  varformula, varUsedRefs, vardesc])
        
        if verbose:
            print(varID + CNseperator + varname + CNseperator + CNvartypes[vartypeid] + CNseperator + varParent + CNseperator + vardesc)

    return repvarlist


def ParseSyncVarFile(varfile, verbose):
# leest de variabelen uit de synced variable file
# return list of: (varID, varname, 'Samengevoegde dimensie', 'Dimension')

    verbose = 0
    syncvarlist = []
    
    filelen = ReadLengthBytes(varfile, 0, 3)

    # CCH 20240605 een property die iets zegt over de bestandsindeling
    # prop = 0: Geen dataprovider ID's (DSx) opgenomen
    # prop = 3: Wel dataprovider ID's opgenomen
    bt_format = varfile[4]
    syncfileformat = int(bt_format)
    
    cntvars = ReadLengthBytes(varfile, 8, 3)

    if verbose:
        print( 'file lengte:', filelen)
        print( 'aantal variabelen:', cntvars)
        print( 'format:', syncfileformat)
        
    startvarblock = 12
    len_varblock = 0
    mypos = startvarblock
    
    
    for varnr in range(cntvars):

        # print(len_varblock)
        if verbose:
            print( '--------------', varnr, startvarblock)
        
        #lees var ID
        varID, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)
        if verbose:
            print()
            print(mypos, mylen, varID)
        
        #lees var naam
        varname, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
        if verbose:
            print(mypos, mylen, varname)
            
        # lees omschrijving
        vardesc, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
        if verbose:
            print(mypos, mylen, vardesc)

        startvarblock = startvarblock + len_varblock
    
        # CCH 20240712 Heel soms staat de nieuwe var op pos + 4 ipv + 8 (LBP020)
        if verbose:
            print('check:', mypos, int(varfile[mypos])) 

        checkvalpos = int(varfile[mypos])
        if checkvalpos==0: 
            # Niets gevonden
            mypos = mypos + 4
        
        # lees DSO
        # CCH 20240605 DSO is kennelijk optioneel  
        if syncfileformat==3:
   
            varDSO, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
    
            if verbose:
                print('DSO',mypos , mylen, varDSO)
       
            #lees # gecombineerder vars
            mypos = mypos + 6
            # print(mypos)

        # CCH 20240712 Vrij lelijke fix nav LBP020 syncfile
        if checkvalpos!=0: 
            # Niets gevonden
            mypos = mypos + 10
            
        cnt_vars = ReadLengthBytes(varfile, mypos, 3)
    
        mypos = mypos + 4
        
        for combvar in range(cnt_vars):
            cvar, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)
            # print(mylen, cvar)
    
            if verbose:
                print( varID + CNseperator + varname + CNseperator + cvar)    

        # Bevinding van Greetje Koers dd 20250319
        # Er worden kennelijk variabelen vastgehouden in de syncfile die geen dimensies meer samenvoegen
        # Vermoedelijk slechte garbage collector van BO
        # Daarom: alleen de syncfile variabele opnemen in de lijst als ie ook daadwerkelijk dimensies samenvoegt (cnt >=2)

        # print('aantal synced vars:', cnt_vars)
        if cnt_vars >= 2:
            syncvarlist.append([varID, varname, 'Samengevoegde dimensie', 'Dimension'])

    return syncvarlist


def getVarNameDictionary(widfilearchive):
# maakt een dictionary van varID->varName voor formule parsing
#    
# NB KOPIE VAN getAllReportVariables
# DIT KAN VAST EFFICIENTER

    verbose = 0

    alldpvars = []

    # mapt de dataprovider ID naar (DP Naam, DP Type, universe naam)
    univdict = getDPUnivMappingFromProps(widfilearchive)

    # CCH 20240610 Ga eerst alle dataprovider files bij langs
    for info in widfilearchive.infolist():
    
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename[-11:] ==  '/' + CNdpvarfilename:

            dpfile = widfilearchive.read(info.filename)
            # BinaryFileViewer(dpfile,'c:\\temp\\output.txt',0)
            dpvars = ParseDataProviderFile(dpfile, 0)

            dpvars_enriched = [ (var[0], var[1], var[2], var[3], univdict[var[3]][0], univdict[var[3]][1], univdict[var[3]][2]) for var in dpvars ]

            alldpvars = alldpvars + dpvars_enriched

    # list of : (objID, objName, 'DataProvider Object',DPID)
    # print(alldpvars)
    
    # Ga alle document variabelen bij langs
    varfile = widfilearchive.read(CNdocvarfilename)
    # list of: (varID, varname,  'Report Variable', CNvartypes[vartypeid], '\'' + varFormulaText, varUsedRefs)
    reportvars = ParseVariableFile(varfile, verbose)

    # print(reportvars)

    # Ga alle samengevoegde (synced) dimensies bij langs
    syncfile = widfilearchive.read(CNsyncvarfilename)
    # list of: (varID, varname, 'Samengevoegde dimensie', 'Dimension')
    syncvars = ParseSyncVarFile(syncfile, 0)
    # print(syncvars)

    # Ga alle referentie variabelen bij langs (zie LRI016)
    try:
        rvfile = widfilearchive.read(CNrefvarfilename)
        refvars = ParseRefVarFile(rvfile, 0)  
    except:
        refvars = []
                
    # Create dictionary varID->varName for formula parsing
    varNameDict = {}
    for var in [ (v[0], v[1]) for v in alldpvars] + [ (v[0], v[1]) for v in reportvars] + [ (v[0], v[1]) for v in syncvars] + [ (v[0], v[1]) for v in refvars] :
        varNameDict[var[0]] = var[1] 

    return(varNameDict)


def getDPVarProperties(widfilearchive, univdict):
# Verzamel de variabelen uit de dataproviders / query objects

    AllDPVars = []

    # Eerst zoeken naar aangepaste DSO descriptions (TD hashtags)
    for info in widfilearchive.infolist():
        if info.filename ==  CNdsmanagerfilename:
            dsmfile = widfilearchive.read(info.filename)

            # Parsen mislukt soms, maar levert niet genoeg ellende op om te stoppen
            
            try:
                DSODescDict = ParseDSManager(dsmfile, 0)
            except:
                DSODescDict = {}
                # print('failed on parsing de DSO manager file (non critical)')
            # print(DSODescDict)
            
    for info in widfilearchive.infolist():

        # print(info.filename, CNdsmanagerfilename)
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename[-11:] ==  '/' + CNdpvarfilename:

            dpfile = widfilearchive.read(info.filename)
            # BinaryFileViewer(dpfile,'c:\\temp\\' + info.filename.replace('/', '') + '.csv',0)
            try:
                dpvars = ParseDataProviderFile(dpfile, 0)
            except:
                dpvars = []

                #  Even voor de herkansing met debugging info aan
                try:
                    dpvars = ParseDataProviderFile(dpfile, 1)
                except:
                    print('debug info gathered...')
                    
                print('failed on dp var parsing in ', info.filename, ' (critical)')

            for var in dpvars:

                DSOVarID = var[0].replace('DP', 'DS')
                if DSOVarID in DSODescDict:
                    DSODesc = DSODescDict[DSOVarID]
                else:
                    DSODesc = '<>'
              
                dpvars_enriched = [(var[0], var[1], DSODesc, var[2], var[3], univdict[var[3]][0], univdict[var[3]][1], univdict[var[3]][2])]
                AllDPVars = AllDPVars + dpvars_enriched
    
    return AllDPVars


def getAllVarProperties(widfilearchive):
# input: wid file archive
# verzamelt alle variabelen die gebruikt worden in een rapport
    verbose = 0

    # mapt de dataprovider ID naar (DP Naam, DP Type, universe naam)
    univdict = getDPUnivMappingFromProps(widfilearchive)

    try:
        alldpvars = getDPVarProperties(widfilearchive, univdict)
    except:
        print('failed on parsing query objects')
    
    
    # Ga alle document variabelen bij langs
    varfile = widfilearchive.read(CNdocvarfilename)
    # list of: (varID, varname,  'Report Variable', CNvartypes[vartypeid], '\'' + varFormulaText, varUsedRefs)

    try:
        reportvars = ParseVariableFile(varfile, verbose)
    except:
        print('failed on reportvars')

    # Ga alle samengevoegde (synced) dimensies bij langs
    syncfile = widfilearchive.read(CNsyncvarfilename)
    # list of: (varID, varname, 'Samengevoegde dimensie', 'Dimension')

    try:
        syncvars = ParseSyncVarFile(syncfile, 0)
    except:
        print('failed on sync vars')
        
    # print(syncvars)

    # Ga alle referentie variabelen bij langs (zie LRI016)

    # Bestand bestaat soms niet in het archief
    try:
        rvfile = widfilearchive.read(CNrefvarfilename)
        refvars = ParseRefVarFile(rvfile, 0)  
    except:
        refvars = []

    # Create dictionary varID->varName for formula parsing
    varNameDict = {}
    for var in [ (v[0], v[1]) for v in alldpvars] + [ (v[0], v[1]) for v in reportvars] + [ (v[0], v[1]) for v in syncvars] + [ (v[0], v[1]) for v in refvars]:
        varNameDict[var[0]] = var[1] 

    # CCH 20250502
    # Inventariseer de groep-variabelen
    # In een aantal gevallen is de groep-variabele in het rapport opgenomen als report variabele met een if-then-else statement
    # Als dat zo is, staat in de groupvarfile een '&' voor de variabele ID en begint de var naam met een spatie
    # In andere gevallen staat de variabele alleen in de groupvar file
    try:
        grpfile = widfilearchive.read(CNgroupvarfilename)
        grpvars = ParseGroupingVarFile(grpfile, varNameDict, 0)  
    except:
        grpvars = []

    
    # print(varNameDict)
    for i, reportvar in enumerate(reportvars):
        varformula = reportvar[4]
        # print(varformula)

        # CCH 20240703 Complexity just counts the number of functions (fnnn) used in a formula
        varFormulaComplexity = len(varformula.split('$f'))-1
        varFormulaText = ParseFormula(varformula, varNameDict)
        # print(varFormulaText)
        reportvars[i].append(varFormulaText)
        reportvars[i].append(varFormulaComplexity)

        # Als er een Groep variabele wordt gevonden met '&' en dezelfde variabele ID, dan de rapport variabele als 'groep variable' betitelen
        groupvarId = '&' + reportvars[i][0]
        if groupvarId in [ (v[0]) for v in grpvars]:
            reportvars[i][2] = 'Groep Variable'

    # layout: [1] VarID, [2] VarName, [3] VarType, [4] Var Formula, [5] Formula complexity, [6] Var description, [7] Var Info (dpname/type), [8] Universe 
    allvarlist = ([ (v[0], v[1], v[3], '<>', 0, v[2], v[5], v[7]) for v in alldpvars]
        + [ (v[0], v[1], v[2], v[7], v[8], v[6], v[3], '<>') for v in reportvars] 
        + [ (v[0], v[1], v[2], v[3], v[4], '<>', v[5], '<>') for v in grpvars if v[0].find('&')<0] 
        + [ (v[0], v[1], v[2], '<>', 0, '<>', v[3], '<>') for v in syncvars]
        + [ (v[0], v[1], v[2], v[3], 0, '<>', '<>', '<>') for v in refvars] )
    
    return allvarlist


def ParseFormula(formula, varNameDict):
# Een formule bestaat uit een aantal tokens welke worden afgesloten met een $-teken a la:
# qDl$f301$oDP0.DO120$s$f302$s$oL1$
# Deze functie retourneert de vertaling van deze functiecodes naar een leesbare formule
# Hiervoor wordt een dictionary gebruikt die variabele ID's vertaald naar namen
# Daarnaast wordt een dictionary (CNformulatokensDict) gebruikt die formule-codes vertaald naar (nederlandse) namen van functies
    
    tokens = formula.split('$')
    formulatext = '\''
    
    objrefs = []
    for token in tokens:
        if len(token)>0:
            tokentype = token[0]
            if tokentype=='f': # function
                try:
                    function = CNformulatokensDict[token]
                except:
                    function = '[' + token + ']'
                    # print(token)
                    
                formulatext = formulatext + function
            elif tokentype=='l': # literal string
                formulatext = formulatext + '"' + token[1::] + '"'
            elif tokentype=='i': # integer
                formulatext = formulatext + token[1::]
            elif tokentype=='r': # real
                formulatext = formulatext + token[1::]   
            elif tokentype=='s': # space
                formulatext = formulatext + ' '
            elif tokentype=='o': # variable
                try:
                    varname = '[' + varNameDict[token[1::]]+ ']'
                except:
                    varname = '[##' + token[1::] + ']'
                    
                formulatext = formulatext + varname
            else: # /n /t etc.
                formultext = formulatext + ' '
                
    return formulatext


def ParseFormulaStructure(formula):
# Een formule bestaat uit een aantal tokens welke worden afgesloten met een $-teken a la:
# qDl$f301$oDP0.DO120$s$f302$s$oL1$
#
# CCH 202250514 Alternatief op 'ParseFormula' met als doel de structuur van de formule te analyseren op overeenkomsten met andere formules
# Plan: variabele aanduiden met <var> / spaties wissen / 
#
    
    tokens = formula.split('$')
    formulatext = '\''
    
    objrefs = []
    for token in tokens:
        if len(token)>0:
            tokentype = token[0]
            if tokentype=='f': # function
                try:
                    function = CNformulatokensDict[token]
                except:
                    function = '[' + token + ']'
                    # print(token)
                    
                formulatext = formulatext + '<' + function + '>'
            elif tokentype=='l': # literal string
                formulatext = formulatext + '<str>'
            elif tokentype=='i': # integer
                formulatext = formulatext + '<int>'
            elif tokentype=='r': # real
                formulatext = formulatext + '<real>'  
            # elif tokentype=='s': # space
                # formulatext = formulatext + ' '
            elif tokentype=='o': # variable
                formulatext = formulatext + '<var>'
            else: # /n /t etc.
                formultext = formulatext + ';'
                
    return formulatext

def getVarRefsFromFormula(formula):
# Een formule bestaat uit een aantal tokens welke worden afgesloten met een $-teken a la:
# qDl$f301$oDP0.DO120$s$f302$s$oL1$
# deze functie retourneert de losse tokens van de formule in een list    
# in een formule wordt de variabele naam soms vooraf gegaan door een 'o', deze halen we er af
    
    tokens = formula.split('$')

    objrefs = []
    for token in tokens:
        if len(token)>0: # Een enkele keer wordt een leeg token meegenomen als de formula eindigt met '$'
            if token[0]== 'o': # Gaat het om een object referentie
                if token.find('DP') == -1 or token.find('.') > -1: # Gaat het om een variabele ref? Kan evt een dataprovider ref zijn.
                    objrefs.append(token[1:])

    #make references unique
    objrefs = list(sorted(set(objrefs)))
    
    return objrefs


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------VARIABLE DEPENDENCIES--------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def GetReportVarDependencies(varfile, vardeps):
# Verzamel alle variabele afhankelijkheden uit formules van variabelen.
# Input: variabele file
# Output: vardeps, lijst van (VARID, DEP VARID, RELATIE TYPE)
    
    filelen = ReadLengthBytes(varfile, 0, 5)
    cntvars = ReadLengthBytes(varfile, 12, 3)

    startvarblock = 16
    len_varblock = 0

    for varnr in range(cntvars):
        
        startvarblock = startvarblock + len_varblock
        
        len_varblock = ReadLengthBytes(varfile, startvarblock, 3)
        
        mypos = startvarblock + 8
        
        #lees var naam
        varname, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
        
        #lees var formule
        varformula, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
        
        #lees var type
        vartypeid = varfile[mypos]
    
        #lees var ID
        mypos = mypos + 2

        varID, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)
        mypos = mypos + 1
        
        refobjs = getVarRefsFromFormula(varformula)
        
        for refobj in refobjs:
            vardeps.append( (varID, refobj,  'formula'))


def GetSyncVarDependencies(varfile, vardeps):
# bepaalt welke dimensies zijn gesynchroniseerd in een synced dimension
# Input: sync variabele file
# Output: vardeps, lijst van (VARID, DEP VARID, RELATIE TYPE (= synced var))
# TECHNICAL DEBT: schaamteloze kopie van ParseSyncVarFile
    
    # BinaryFileViewer(varfile, 'c:\\temp\\exp.txt', 0)
    verbose = 0

    syncvarlist = []
    
    filelen = ReadLengthBytes(varfile, 0, 3)
    cntvars = ReadLengthBytes(varfile, 8, 3)

    # CCH 20240605 een property die iets zegt over de bestandsindeling
    # prop = 0: Geen dataprovider ID's (DSx) opgenomen
    # prop = 3: Wel dataprovider ID's opgenomen
    bt_format = varfile[4]
    syncfileformat = int(bt_format)

    if verbose:
        print( 'file lengte:', filelen)
        print( 'aantal variabelen:', cntvars)
        print( 'format:', syncfileformat)
        
    startvarblock = 12
    len_varblock = 0
    mypos = startvarblock
    
    for varnr in range(cntvars):

        #lees var ID
        varID, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)
        
        #lees var naam
        varname, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)
     
        # lees omschrijving
        vardesc, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)

        syncvarlist.append([varID, varname, 'Samengevoegde dimensie', 'Dimension'])

        startvarblock = startvarblock + len_varblock

    
        checkvalpos = int(varfile[mypos])
        if checkvalpos==0: 
            # Niets gevonden
            mypos = mypos + 4
        
        # lees DSO
        # CCH 20240605 DSO is kennelijk optioneel  
        if syncfileformat==3:
 
            varDSO, mypos = ReadObject(varfile, mypos, 4, 2, 0, 0)

            #lees # gecombineerder vars
            mypos = mypos + 6
            # print(mypos)

        # CCH 20240712 Vrij lelijke fix nav LBP020 syncfile
        if checkvalpos!=0: 
            # Niets gevonden
            mypos = mypos + 10
            
        cnt_vars = ReadLengthBytes(varfile, mypos, 3)
    
        mypos = mypos + 4
        
        for combvar in range(cnt_vars):
            cvar, mypos = ReadObject(varfile, mypos, 1, 1, -1, 0)

            vardeps.append( (varID, cvar,  'synced var') ) 


def GetAllDirectReportDependencies(mywidfile, varfilelist, vardeps):
# Verzamelt alle direct afhankelijkheden van variabelen aan de rapport definitie
# vardeps wordt 'by reference' meegegeven in deze functie, zodat eventuele vulling vooraf van de vardeps niet verloren gaat
    
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


def GetDPVarDependencies(widfilearchive):
# CCH 20250319 haalt de afhankelijkheden tussen dataproviders op.
# In een dataprovider kan in het filter blok een object uit een andere query worden gebruikt om te filteren

    varDPdeps = []
    for info in widfilearchive.infolist():
    
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename[-11:] ==  '/' + CNdpvarfilename:
    
            dpfile = widfilearchive.read(info.filename)

            # haal de DPID uit het pad waarin de CNdpvarfilename staat
            # pad = Data/C3/DATAPROVIDERS/ [DP0/] DPGeneric
            DPID = info.filename[22:26].replace('/','')
            # print(DPID)
            # print()
            myxml = GetDataProviderXML(dpfile)

            try:
                xmldoc = ET.ElementTree(ET.fromstring(myxml))
    
                root = xmldoc.getroot()
    
                for elem in root.findall('.//condition//operands'):
                # Op zoek naar de DataProviderOperand, waarmee een depency tussen query's ontstaat
    
                    # even iets netter dan try/except
                    if CNXMLKeyType in elem.attrib:
                        if elem.attrib[CNXMLKeyType] == 'queryspec:DataProviderOperand':
    
                            # onder de aanname dat de key referencedDPObject altijd bestaat...
                            refVarID = elem.attrib['referencedDPObject']
    
                            # DP moet zijn DPID (dp0 / 1 etc)
                            varDPdeps.append( (DPID , refVarID,  'dataprovider filter') ) 
            except:
                # xml was niet te parsen in een enkel geval,
                # dummy commando om de exceptie te negeren
                nogo = 1
                
    return varDPdeps


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


def MapDPVarDependencies(allvarlist, DPdeps, vardeps):
# vertaalt de DPID dependency naar individuele bij deze DPID behorende DP Object dependencies
# en neemt deze op in de vardeps

    for DPdep in DPdeps:
        DPIDdep = DPdep[0]

        for var in allvarlist:
            if var[0].find(DPIDdep + '.')>-1:
                vardeps.append( (var[0] , DPdep[1],  'dataprovider filter') ) 
            

def getAllVariableDepencencies(widfilearchive, allvarlist):
# Haalt uit 3 bronnen alle variabele afhankelijkheden op:
# * Direct report * Variable formula and master detail * synchronized dimensions
# output formaat: <van var>, <naar var>, <bron>
    
    # dit is een list van tuples (var, vardep)
    vardeps = []
    
    # directe rapport afhankelijkheden

    GetAllDirectReportDependencies(widfilearchive, allvarlist, vardeps)


    # afhankelijkheden via formules en master-detail
    varfile = widfilearchive.read(CNdocvarfilename) 
    GetReportVarDependencies(varfile, vardeps)
 
    # afhankelijkheden door samengevoegde dimensies
    syncfile = widfilearchive.read(CNsyncvarfilename)  
    GetSyncVarDependencies(syncfile, vardeps)
        
    # afhankelijkheden tussen dataprovider door filters op basis van waarden uit andere query objecten

    DPdeps = GetDPVarDependencies(widfilearchive)
    
    # nu hebben we een lijstje dat DPID's mapt naar objectID uit een andere query
    # om hier een nette var dependency van te maken, creeeren we voor elke object in de DPID dataprovider een losse
    # dependency naar objectID
    MapDPVarDependencies(allvarlist, DPdeps, vardeps)
   
    # ontdubbelen (misschien onnodig)
    vardeps = list(set(vardeps))
    
    return vardeps


def getReportVarsAndDependencies(widfilearchive, varlist, showalldeps = False):
# bepaalt van alle variabelen in de varlist welke afhankelijkheden er zijn
# showalldeps: True: laat alle paden zien / False: toon alleen het kortste pas van var naar rapport

    vardepslist = getAllVariableDepencencies(widfilearchive, varlist)


    vardepstree = []

    for i, repvar in enumerate(varlist):

        varID = repvar[0]
        myvardep = getVarDependency(varID, vardepslist, varID, 1)


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

                if min_repdepth == 1:
                    deptype = 'Direct'
                else:
                    deptype = 'Indirect'
                
                    
                for dep in myvardep:

                    #laat alle dependency paden zien van de variabele
                    if showalldeps:
                        vardepstree.append( (repvar[0], repvar[1], repvar[2], repvar[3], repvar[4], repvar[5] , repvar[6], repvar[7], deptype, dep[2], 1 ) )
                    #laat alleen het kortste dependency pad zien van variabele naar report    
                    else:
                        if dep[3] == 'report':
                            if dep[4] == min_repdepth and showsample==1:
                            # als het korste pad is gevonden    
                                showsample = 0
                                vardepstree.append( (repvar[0], repvar[1], repvar[2], repvar[3], repvar[4], repvar[5] ,repvar[6], repvar[7], deptype, dep[2], str(cnt_deps) ) )
                            
            else:
            # variabele zonder pad naar report
                for j, dep in enumerate(myvardep): 
                    if j==0:
                        vardepstree.append( (repvar[0], repvar[1], repvar[2], repvar[3], repvar[4], repvar[5], repvar[6], repvar[7], 'Geen', dep[2], str(cnt_deps) ))                    
        else:
            vardepstree.append( ( repvar[0], repvar[1], repvar[2],  repvar[3], repvar[4], repvar[5], repvar[6], repvar[7], 'Geen','' ,'0') )

    df_vardeps = pd.DataFrame(vardepstree, columns =['Var ID', 'Var Name', 'Var Type', 'Var Formula', 'Complexity', 'Description', 'Var Info', 'Universe','Report dependency', 'Dep. path', '# Deps'])

    return df_vardeps


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------- OVERIGE HULP FUNCTIES -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  
    
def etree_iter_path(node, tag=None, path='.', freezepath = 0):
# Recursieve functie om alle Xpaden in een XML rapport definitie op te halen
# Wordt gebruikt om te tonen waar rapport variabelen worden gebruikt in het rapport
    
    if tag == "*":
        tag = None
        
    if tag is None or node.tag == tag:
        yield node, path

    # In sommige gevallen de naam van de tag tonen
    # zodat het duidelijk is waar in het rapport de afhankdelijkheid zit
    tag_name =  ''
    param_name = ''
    if node.tag in CNShowTagNameList:
        
        try:
            tag_name = json.loads(node[0].find(".//PVAL[@NAME='name']").text)['l']
            # print(tag_name)
        except:
            tag_name = ''
            
        try:
            tag_subtype = node[0].find(".//PVAL[@NAME='subtype']").text + ':'
            # print(tag_subtype)
        except:
            tag_subtype = ''

    if len(tag_name) > 0:
        path = path + '[' + tag_subtype + tag_name + ']'
           
    for child in node:

        fp = freezepath
        if node.tag in FreezePathList:
            fp = 1

        child_path = path
            
        if fp==0:

            # bepaal de naam van de parameter 
            child_info = child.tag
            
            if child.tag == 'PVAL':
                try:
                    param_name = child.attrib['NAME']
                except:
                    param_name = ''
                    
            if len(param_name) > 0:
                child_info = child_info + '[' + param_name + ']'

            child_path = path + '/' + child_info

        for child, child_path2 in etree_iter_path(child, tag, path = child_path, freezepath = fp):
            yield child, child_path2


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
            formvars = getVarRefsFromFormula(formula)
            if len(formvars)>0:
                xPaths.append((path, elem.tag  , formula, 'pval tag', formvars))
        
        if (elem.tag =='PVAL' and elem.attrib['NAME']=='content'):
            tag_name = json.loads(elem.text)
            if tag_name['type']== 'formula':
                formula = tag_name['str']
                formvars = getVarRefsFromFormula(formula)
                # print(path)
                if len(formvars)>0:
                    xPaths.append((path, elem.tag  ,formula , 'pval tag', formvars))
                    # print('we hebben formvars:', path, formula)
            
        if elem.tag == 'PLUGINFO':
        # CCH 20240429 some CDATA in here, so give it a special treatment
            xmlplugin = ET.ElementTree(ET.fromstring(elem.text))
            for elemplugin in xmlplugin.iter():
                
                if elemplugin.tag == 'property':
                    # print(elemplugin)    
                    if elemplugin.attrib['key']== 'formula':
                        # print(elemplugin.attrib['value'])
                        formula = elemplugin.attrib['value']
                        formvars = getVarRefsFromFormula(formula)
                        if len(formvars)>0:
                            xPaths.append((path, elem.tag  ,formula , 'pval tag', formvars)) 
            
        # doorzoek de attribuut waarden (zoals bij alerters)
        # NB hit al dan niet met voorloop  'o' plus varID
        for att in elem.attrib:

            if att in ( 'KEY'):
                xPaths.append((path, elem.tag + ': ' + att, elem.attrib[att], 'att', [elem.attrib[att]]))


            if att in ( 'EXPR') and elem.tag == 'EMPT_COND':
                formula = elem.attrib[att]
                formvars = getVarRefsFromFormula(formula)
                if len(formvars)>0:
                    xPaths.append((path, elem.tag  ,formula , 'pval tag', formvars))               
                    
            if att in ( 'EXPRESSION'):
                formula = elem.attrib[att]
                formvars = getVarRefsFromFormula(formula)
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
                        if form_att in ( 'ID',  'BINDOBJECT', 'VARIABLEIDASDEFAULTVALUES'):
                            # CCHX
                            # print((path + ' Besturingselement ', form_att, form_el.attrib[form_att], 'form_att', [form_el.attrib[form_att]]))
                            xPaths.append((path + ' Besturingselement ', form_att, form_el.attrib[form_att], 'form_att', [form_el.attrib[form_att]]))
                            # mydeps.append(path +  ' Besturingselement ' + ' [' + form_el.attrib[form_att] +']')
    
    # return list(set(xPaths))
    return xPaths


def getReportXML(widfilearchive):
# Haal de XML op van een rapport, in XML formaat
    bt_docspecfile = widfilearchive.read(CNdocspecfilename)
   
    docspec = bt_docspecfile.decode(CNcoding)
    
    # skip some initial bytes before report tag starts
    docspec = docspec[8:]
    xmldoc = ET.ElementTree(ET.fromstring(docspec))
    
    return xmldoc


def getReportXMLString(widfilearchive):
# Haal de XML op van een rapport, in string formaat
    bt_docspecfile = widfilearchive.read(CNdocspecfilename)
    
    docspec = bt_docspecfile.decode(CNcoding)
    
    # skip some initial bytes before report tag starts
    docspec = docspec[8:]

    return docspec


def dumpReportXMLString(widfilearchive, outputfile):
# Schrijft de rapport definitie XML naar een opgegeven outputfile
    
    flatfile = open( outputfile, mode =  'a', encoding='utf-8')

    xml_string = getReportXMLString(widfilearchive)
    
    flatfile.write(xml_string)

    flatfile.close()


def getAlerterStatus(widfilearchive):
# bepaalt van alle alerters in de alert lijst van het report of deze wel worden gebruikt
    
    # get all alerters
    alerterlist = []

    xmldoc = getReportXML(widfilearchive)
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


def getReportDocSpecStats(widfilearchive):
# Haalt verschillende statistieken uit de rapport definitie (docspec)

    counttabs = 0
    specstats = {}
    cnspecstats = {}
    
    xmlstr = getReportXMLString(widfilearchive)
    xmlsize = int(len(xmlstr)/1024)
    
    xmldoc = ET.ElementTree(ET.fromstring(xmlstr))

    root = xmldoc.getroot()
    
    reporttabs = root.iterfind('./REPORT/PLIST/PVAL[@NAME="name"]')

    # for tab in reporttabs:
    #     tabname = json.loads(tab.text)['l']
    #     print(tabname)
        
    specstats['# Tabs'] = sum(1 for _ in reporttabs)
    
    for tag in CNGetStats:
        results = root.iterfind('.//' + tag)

        cnt = sum(1 for _ in results)

        cnspecstats[tag] = cnt
        
    specstats['# Tabellen'] = cnspecstats[CNGetStats[0]] + cnspecstats[CNGetStats[1]]
    specstats['# Grafieken'] = cnspecstats[CNGetStats[2]]
    specstats['Size (KB)'] = xmlsize

    return specstats


def GetVarReportUsage(widfilearchive):
#  input: widfile archive

    # docspecstring = getReportXMLString(myWebiPathReport)
    xmldoc = getReportXML(widfilearchive)
    varlist = getAllVarProperties(widfilearchive)

    xpaths = getReportXPaths(xmldoc)

        
    varreportusage =[]
    
    for i, myvar in enumerate(varlist):
        varID = myvar[0]
    
        varpaths = []
        for varpath in xpaths:
            foundvars = varpath[4]
            if varID in foundvars:
                varpaths.append(varpath[0])
    
        # remove duplicates
        varpaths = list(set(varpaths))
        
        for xpath in varpaths:
            varreportusage.append((myvar[0], myvar[1], myvar[2], myvar[4], xpath))
    
    df_varreportusage = pd.DataFrame(varreportusage, columns =['Var ID', 'Name', 'Type', 'Info', 'Report XPath'])

    return df_varreportusage


def AnalyzeReport(widfilearchive, showalldeps):
# Algemene functie om een WID file te analyseren, output van de functie gaat naar xls-tabs
#
# input: widfile archive
# input: showalldeps: 0: laat alleen het langste dependency pad zien in de afhankelijkheden. 1: laat alle afhankelijkheden zien
# output: lijst met variabele properties+ afhankelijkheden aanduiding / lijst met variabele gebruik in rapport XML / lijst met alerter gebruik

    xmldoc = getReportXML(widfilearchive)

    # Verzamel alle rapport variabelen (code / omschrijving / eigenschappen)
    # * Query objecten / Dataprovider objecten
    # * Reguliere variabele
    # * Referentie variabele
    # * Groepering variabele
    # * Samengevoegde dimensie
    
    varlist = getAllVarProperties(widfilearchive)

    # Bepaal de variabele afhankelijkheden

    df_vardependencies = getReportVarsAndDependencies(widfilearchive, varlist, showalldeps)      

    # Bepaal waar in de rapport definitie (xml) variabelen worden gebruikt
    xpaths = getReportXPaths(xmldoc)

    varreportusage =[]
    
    for i, myvar in enumerate(varlist):
        varID = myvar[0]
    
        varpaths = []
        for varpath in xpaths:
            foundvars = varpath[4]
            if varID in foundvars:
                varpaths.append(varpath[0])
    
        # remove duplicates
        varpaths = list(set(varpaths))
        
        for xpath in varpaths:
            varreportusage.append((myvar[0], myvar[1], myvar[2], myvar[4], xpath))

    
    df_varreportusage = pd.DataFrame(varreportusage, columns =['Var ID', 'Name', 'Type', 'Info', 'Report XPath'])

    alert_stat = getAlerterStatus(widfilearchive)
    df_alerters = pd.DataFrame(alert_stat, columns =['Alerter ID', 'Alerter Name', 'Status'])

    return df_vardependencies, df_varreportusage, df_alerters


def GetReportStats(df_allvardeps):
# Verzamelt verschillende statistieken van een rapport:
# ['# Universes', '# Queries', '# Variables', '# Junk', 'avg FC', 'max FC']
# CCH 20250513 Primaire sleutel ReportSUID niet altijd bijgewerkt -> terug naar rapportnaam
  
    ucnt_universes = df_allvardeps.groupby('ReportName')['Universe'].nunique()-1

    reportstats = ucnt_universes.to_frame()
    
    # filter alleen query objecten
    univ_obj = df_allvardeps[df_allvardeps['Universe']!='<>']
    ucnt_queries = univ_obj.groupby('ReportName')['Var Info'].nunique()

    reportstats = reportstats.merge(ucnt_queries.to_frame(), how='left', left_index=True, right_index=True)
    
    cnt_queryobjects =  univ_obj.groupby('ReportName').size()
    reportstats = reportstats.merge(cnt_queryobjects.to_frame(), how='left', left_index=True, right_index=True)
    
    repvars = df_allvardeps[df_allvardeps['Universe']=='<>']
    cnt_vars = repvars.groupby('ReportName').size()
    reportstats = reportstats.merge(cnt_vars.to_frame(), how='left', left_index=True, right_index=True)
    
    # filter alleen ongebruikte variabelen
    junkvars = df_allvardeps[df_allvardeps['Report dependency']=='Geen']
    cnt_junkvars = junkvars.groupby('ReportName').size()

    reportstats = reportstats.merge(cnt_junkvars.to_frame(), how='left', left_index=True, right_index=True)

    # reportvars['Complexity'] = reportvars['Complexity'].astype(str).astype(int)
    repvars.insert(4, 'Complexity Numeric', repvars['Complexity'].astype(str).astype(int) )
    
    avg_complex= repvars.groupby('ReportName')['Complexity Numeric'].mean()
    max_complex= repvars.groupby('ReportName')['Complexity Numeric'].max()

    reportstats = reportstats.merge(avg_complex.to_frame(), how='left', left_index=True, right_index=True)
    reportstats = reportstats.merge(max_complex.to_frame(), how='left', left_index=True, right_index=True)
        
    reportstats.columns=['# Universes', '# Queries', '# Q Objects', '# Variables', '# Junk', 'avg FC', 'max FC']

    # afgeleide statistiek invoegen
    reportstats.insert(4, '% Junk', 100.0 * reportstats['# Junk'] / (reportstats['# Variables'] + reportstats['# Q Objects']))

    # reportstats['% Junk'] = reportstats['% Junk'].astype(int)

    return reportstats

def getReportTDTag(docprops):
# Zoekt in de rapport description of er geaccepteede technical debt in staat
    
    tdtag = ''
    if 'description' in docprops:
        description = docprops['description']
        if description.find('#TD')> -1:
            for descline in description.splitlines():
                if descline.find('#TD')> -1:
                    tdtag = descline

    return tdtag

def GetReportFormulas(widfilearchive):
#  input: widfile archive
#  look for swerving formula's without var definition

    formulas = []
    docprops = ReadDocumentProps(widfilearchive)
    reportName = docprops[CNpropreportname]
    
    # CCH 20250409 op 1 rapport na altijd gevuld
    try:
        reportSUID = docprops['SI_CUID']
    except:
        reportSUID = '<n/a>'


    xmldoc = getReportXML(widfilearchive)
    xpaths = getReportXPaths(xmldoc)

    varDict = getVarNameDictionary(widfilearchive)

    for xp in xpaths:
        formula = xp[2]
        formulaComplexity = len(formula.split('$f'))

        # negeer formules met beperkte complexiteit
        if formulaComplexity > CNswervingformulacomplexity:
            formulaText = ParseFormula(formula, varDict)
            formulas.append((reportSUID, reportName, xp[0], formulaText, formulaComplexity))

    df_formulas = pd.DataFrame(formulas, columns =['Report SUID', 'Report', 'Location', 'Formula', 'Complexity'])
    
    return df_formulas

def GetSwervingFormulas(widpath):
# Verzamelt alle los zwevende formules in een rapport, dus welke niet in een variabele zijn gezet.
    
    allformulas = []
    widfiles = widpath +  '*.wid'
    for widfile in glob.glob(widfiles):

        widfilearchive = zf.ZipFile(widfile, mode='r')
        docprops = ReadDocumentProps(widfilearchive)
        reportName = docprops[CNpropreportname]
        
        # CCH 20250409 op 1 rapport na altijd gevuld
        try:
            reportSUID = docprops['SI_CUID']
        except:
            reportSUID = '<n/a>'
            
        try:
            df_formulas = GetReportFormulas(widfilearchive)
            # print(formulas)
            allformulas.append(df_formulas)
        except:
            print(reportSUID, reportName, '(', widfile, ')', 'crash on swerving formulas')
            
        widfilearchive.close()

    df_allformulas = pd.concat(allformulas)

    return df_allformulas


def checkVarFormulaRefs(widfilearchive):
# Controleert of variabelen waarnaar in formules van rapport-variabelen wordt verwezen uberhaupt bestaan
# input: wid file archive
# output: lijst met niet bestaande gerefereerde variabelen.    

    verbose = 0

    alldpvars = []

    docprops = ReadDocumentProps(widfilearchive)
    reportName = docprops[CNpropreportname]    

    # CCH 20250409 op 1 rapport na altijd gevuld
    try:
        reportSUID = docprops['SI_CUID']
    except:
        reportSUID = '<n/a>'


    # mapt de dataprovider ID naar de universe naam
    univdict = getDPUnivMappingFromProps(widfilearchive)

    # CCH 20240610 Ga eerst alle dataprovider files bij langs
    for info in widfilearchive.infolist():
    
        # Als er een dp generic file gevonden is, dan uitlezen
        if info.filename[-11:] ==  '/' + CNdpvarfilename:

            dpfile = widfilearchive.read(info.filename)
            # BinaryFileViewer(dpfile,'c:\\temp\\output.txt',0)
            dpvars = ParseDataProviderFile(dpfile,0)

            alldpvars = alldpvars + dpvars

    # print(alldpvars)
    
    # Ga alle document variabelen bij langs
    varfile = widfilearchive.read(CNdocvarfilename)
    reportvars = ParseVariableFile(varfile, verbose)

    # print(reportvars)

    # Ga alle samengevoegde (synced) dimensies bij langs
    syncfile = widfilearchive.read(CNsyncvarfilename)
    
    syncvars = ParseSyncVarFile(syncfile, 0)

    # print(syncvars)

    # allvarlist = [ (v[0], v[1], v[2]) for v in alldpvars] +  [ (v[0], v[1], v[2]) for v in reportvars] +  [ (v[0], v[1], v[2]) for v in syncvars]
    allvarlist = [ (v[0]) for v in alldpvars] +  [ (v[0]) for v in reportvars] +  [ (v[0]) for v in syncvars]
    # print(allvarlist)
    
    exceptVarList = []
    for reportvar in reportvars:
        # print(reportvar)
        refVars = reportvar[5]
        for refVar in refVars:
            if refVar not in allvarlist:
                exceptVarList.append((reportSUID, reportName, reportvar[0], reportvar[1], refVar))

    df_deadrefs = pd.DataFrame(exceptVarList, columns =['Report SUID', 'Report Name', 'Var ID', 'Var Name', 'Invalid Var reference'])

    
    return df_deadrefs


def GetInvalidVarReferences(widpath):
# Verzamelt alle ongeldige variabele verwijzigingen uit wid files in een bepaalde map
    
    allinvalidrefs = []
    widfiles = widpath +  '*.wid'

    for widfile in glob.glob(widfiles):

        try:
            widfilearchive = zf.ZipFile(widfile, mode='r')
        
            df_evars = checkVarFormulaRefs(widfilearchive)
               
            allinvalidrefs.append(df_evars)
            
        except:
            print(widfile,'crash on invalid refs')            

    df_allinvalidrefs = pd.concat(allinvalidrefs)

    return df_allinvalidrefs
    
