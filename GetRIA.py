#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import urllib
import urllib2
import zipfile
import cStringIO

import numpy as np
import pylab



def get_RIA ( prov, est, fInicio, fFin, variables=None, url=None ):
    """Download data from RIA. We need a province, a station number,
    as a start and end dates. The user may also change the URL, and 
    select what variables will be downloaded.

    Parameters
    ============
    prov: int
          The province code
    est: int
          The station code. See `<http://www.juntadeandalucia.es/agriculturaypesca/ifapa/ria/servlet/FrontController>`_ for details
    fInicio: str
            The starting date
    fFin: str
            The end date
    url: str
            Base URL
    variables: list
        List of variables to download

    __author__: J Gómez-Dans, NCEO/UCL
    __email__: j.gomez-dans@ucl.ac.uk
    """

    if url is None:
         url = "http://www.juntadeandalucia.es"+\
           "/agriculturaypesca/ifapa/ria/servlet/"+\
           "FrontController?action=Download"+\
           "&url=descargarDatosEstaciones.jsp&"+\
           '&c_provincia=%02d&c_estacion=%d&zInicio=%s' % \
           ( prov,est,fInicio ) + '&zFin=%s' % ( fFin )
    if variables is None:
        variables = [ 'TempMax', 'TempMin', 'TempMedia', \
                      'HumedadMax', 'HumedadMin', 'HumedadMedia',
                      'Radiacion', 'VelViento', 'DirViento', \
                      'Precipitacion', 'ETo' ]
    vari = "".join ( [ "&%s=S"%s for s in variables] )
    url = url + vari
    req = urllib2.urlopen ( url )
    data = req.read()
    zdata = cStringIO.StringIO( data )
    zipo = zipfile.ZipFile ( zdata )
    for fich in zipo.NameToInfo.keys():
        s = zipo.read ( fich )
    zipo.close()
    datos = s.split ("\r\n" )
    datos.pop(1)
    return datos
        
    
def get_RAIF ( est, fInicio, fFin, variables=None, url=None ):
    """Download data from RAIF. We need a station number. As of the time
    of writing, they go from 1 to 89. Also need start and end dates. 
    The user may also change the URL, and select what variables will be 
    downloaded.

    Parameters
    ============
    est: int
          The province code
    fInicio: str
            The starting date
    fFin: str
            The end date
    url: str
            Base URL
    variables: list
        List of variables to download

    __author__: J Gómez-Dans, NCEO/UCL
    __email__: j.gomez-dans@ucl.ac.uk
    """
    import urllib2
    import cStringIO
    import zipfile
    if url is None:
        # Start by ignoring the HTTPs
        # Una fiesta esto...
        url = "http://ws128.juntadeandalucia.es/agriculturaypesca/" + \
              "fit/clima/datos.excel.descarga.do?"
    url = url + "id=%d" % est
    ( d1, m1, a1 ) = fInicio.split( "-" )
    ( d2, m2, a2 ) = fFin.split( "-" )
    url = url + "&d1=%s&m1=%s&a1=%s" % ( d1, m1, a1 )
    url = url + "&d2=%s&m2=%s&a2=%s" % ( d2, m2, a2 )
    if variables is None:
        # retruécano... qué cohones de variable tenemos aquí?
        variables = [ "tmax", "tmin", "tmed", "hmax", "hmed", \
             "hmin", "rs", "llc", "llt", "smax", "smed", "smin", \
             "vd", "vv" ]
    vari = "".join ( [ "&%s=S"%s for s in variables] )
    url = url + vari
    req = urllib2.urlopen ( url )
    data = req.read()
    return data
    
def process_data_ria ( data ):
    """This function proesses the data bundle downloaded from a RIA
    station, and returns a number of arrays with the data."""

    t_string = [ line.split()[0] for line in data[2:] \
            if len(line.split()) == 13 ]
    year = [ 2000 + int ( line.split()[0].split( "-" )[-1] ) \
            for line in data[2:] if len(line.split()) == 13 ]
    doy = [ int ( line.split()[1]) \
            for line in data[2:] if len(line.split()) == 13 ]
    tmax = [ float ( line.split()[2]) \
            for line in data[2:] if len(line.split()) == 13 ]
    tmin = [ float ( line.split()[4]) \
            for line in data[2:] if len(line.split()) == 13 ]
    tmean = [ float ( line.split()[6]) \
            for line in data[2:] if len(line.split()) == 13 ]
    swrad = [ float ( line.split()[-3]) \
            for line in data[2:] if len(line.split()) == 13 ]
    wspd = [ float ( line.split()[-5]) \
            for line in data[2:] if len(line.split()) == 13 ]
    eto = [ float ( line.split()[-1]) \
            for line in data[2:] if len(line.split()) == 13 ]
    hum = [ float ( line.split()[9]) \
            for line in data[2:] if len(line.split()) == 13 ]
    prec = [ float ( line.split()[-2]) \
            for line in data[2:] if len(line.split()) == 13 ]
    t_axis = [ time.strftime("%Y-%m-%d", \
            time.strptime( line.split()[0], "%d-%m-%y")) \
            for line in data[2:] if len(line.split()) == 13 ]
    t_axis = pylab.datestr2num ( t_axis )
    return ( t_axis, t_string,year, doy, tmax, tmin, tmean, swrad, \
            wspd, eto, hum, prec )

def process_data_raif ( data ):
    """This function proesses the data bundle downloaded from a RIA
    station, and returns a number of arrays with the data."""
    
    t_string = []
    year = []
    doy = []
    tmax = []
    tmin = []
    tmean = []
    prec = []
    hum = []
    swrad = []
    wspd = []
    t_axis = []
    
    for line in data[1:]:
        if len( line.strip().split()) == 18:
            this_date = line.split()[0].replace("/", "-")
            t_string.append ( this_date )
            year.append ( int ( line.split()[0].split( "/" )[-1] ) )
            doy.append ( int (time.strftime("%j", \
                    time.strptime( this_date, "%d-%m-%Y"))))
            try:
                tmax.append( float ( line.split()[1].replace(",",".")) )
            except ValueError:
                tmax.append ( -999.99 )
            try:
                tmin.append( float ( line.split()[3].replace(",",".")) )
            except ValueError:
                tmin.append ( -999.99 )
                
            try:
                tmean.append(float ( line.split()[2].replace(",",".")) )
            except ValueError:
                tmean.append ( -999.99 )
            try:
                prec.append (float ( line.split()[4].replace(",",".")) )
            except ValueError:
                prec.append ( -999.99 )
            try:
                hum.append ( float ( line.split()[6].replace(",",".")) )
            except ValueError:
                hum.append ( -999.99 )
            try:
                swrad.append(float ( line.split()[8].replace(",",".")) )
            except ValueError:
                swrad.append ( -999.99 )
            try:
                wspd.append(float ( line.split()[-1].replace(",",".")) )
            except ValueError:
                wspd.append ( -999.99 )
    t_axis = [ time.strftime("%Y-%m-%d", \
                time.strptime( this_date, "%d-%m-%Y")) \
                for this_date in t_string]
    t_axis = pylab.datestr2num ( t_axis )
    
    t_axis = np.array ( t_axis )
    t_string = np.array ( t_string )
    year = np.array ( year )
    doy = np.array ( doy )
    tmax = np.array ( tmax )                
    tmin = np.array ( tmin )
    tmean = np.array ( tmean )
    swrad = np.array ( swrad )
    wspd = np.array ( wspd )
    prec = np.array ( prec )
    hum = np.array ( hum )
    
    return ( t_axis, t_string,year, doy, tmax, tmin, tmean, swrad, \
                    wspd,  hum, prec )
                    

def download_all_ria ( start_date="01-01-2000", fname_out = "RIA_data.txt", \
            today = time.strftime("%d-%m-%Y",time.localtime())):
    """This function dowloads data from **ALL** RIA stations. All the
    arguments are optional, but they let you define the temporal period
    to consider and the output filename.
    
    Parameters
    -------------
    start_date: str
                The start date
    fname_out: str
                The output filename
    today: str
                 The end dat
    """
    # Get stations...
    url = "http://raw.github.com/gist/4252296/fb8aaf71531eb936c27f93b33e623d4b9fac6009/ria.txt"
    req = urllib2.urlopen ( url )
    data = req.read()
    stations = cStringIO.StringIO( data )
    stations = np.loadtxt ( stations ).astype(np.int8)
    
    fp = open ( fname_out, 'w' )
    fp.write("# RIA data downloader. By J Gomez-Dans (NCEO/UCL)\n")
    fp.write("# For questions, email j.gomez-dans.ucl.ac.uk\n")
    fp.write("# Started @ %s\n" % time.asctime() )
    fp.write("#Province Station Date Year DoY DateStr TMax Tmin TMean SWRad WSpd ETo Hum Prec\n")
    d_stations=0
    for (prov, est ) in stations:
        print "Doing station %d-%d" % ( prov, est)
        try:
            ria = get_RIA( prov, est, start_date, today )
        except:
            print "\t... Skipping station"
            continue
        d_stations = d_stations + 1
        print "\t... Data downloaded"
        ( t_axis, t_string,year, doy, tmax, tmin, tmean, swrad, \
                            wspd, eto, hum, prec ) = process_data_ria ( ria )
        for i in xrange(len(t_axis)):
            fp.write("%d %d %s %d %d %8.0f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f\n" % \
                    ( prov, est, t_string[i], year[i], doy[i], t_axis[i], \
                    tmax[i], tmin[i], tmean[i], swrad[i], wspd[i], \
                    eto[i],hum[i],prec[i] ))
        fp.flush()
        print "\t... Wrote station"
    fp.write ( "# Downloaded %d stations\n" % d_stations)
    fp.write ( "# Finished on %s\n" % time.asctime() )
    fp.close()

def download_all_raif (  start_date="01-01-2000", fname_out = "RAIF_data.txt", \
            today = time.strftime("%d-%m-%Y",time.localtime())):
    """This function dowloads data from **ALL** RAIF stations. All the
    arguments are optional, but they let you define the temporal period
    to consider and the output filename.
    
    Parameters
    -------------
    start_date: str
                The start date
    fname_out: str
                The output filename
    today: str
                 The end date
    """    
        
    fp = open ( fname_out, 'w' )
    fp.write("# RAIF data downloader. By J Gomez-Dans (NCEO/UCL)\n")
    fp.write("# For questions, email j.gomez-dans.ucl.ac.uk\n")
    fp.write("# Started @ %s\n" % time.asctime() )
    fp.write("#Station Date Year DoY DateStr TMax Tmin TMean SWRad WSpd ETo Hum Prec\n")
    d_stations=0
    today = time.strftime("%d-%m-%Y",time.localtime())
    for station in xrange(1,90):
        print "Doing station %s" % ( str (station))
        try:
            raif = get_RAIF ( station, start_date, today )
        except:
            print "\t... Skipping station"
            continue
        d_stations = d_stations + 1
        print "\t... Data downloaded"
        ( t_axis, t_string,year, doy, tmax, tmin, tmean, swrad, \
                            wspd,  hum, prec ) = process_data_raif  ( raif )
        for i in xrange(len(t_axis)):
            fp.write("%d %s %d %d %8.0f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f \n" % \
                    ( station, t_string[i], year[i], doy[i], t_axis[i], \
                    tmax[i], tmin[i], tmean[i], swrad[i], wspd[i], \
                    hum[i],prec[i] ))
        fp.flush()
        print "\t... Wrote station"
    fp.write ( "# Downloaded %d stations\n" % d_stations)
    fp.write ( "# Finished on %s\n" % time.asctime() )
    fp.close()

        


def worker ( network ):
    """thread worker function"""
    print "Doing %s" % network
    if network == "RIA":
        download_all_ria ()
    elif network  == "RAIF":
        download_all_raif ()
    return

if __name__ == "__main__":
    import multiprocessing
    jobs = []
    for i in ["RIA", "RAIF"]:
        p = multiprocessing.Process( target=worker, args=(i,) )
        jobs.append(p)
        p.start()
    