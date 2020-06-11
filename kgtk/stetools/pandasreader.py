############################# BEGIN LICENSE BLOCK ############################
#                                                                            #
# Copyright (C) 2020                                                         #
# UNIVERSITY OF SOUTHERN CALIFORNIA, INFORMATION SCIENCES INSTITUTE          #
# 4676 Admiralty Way, Marina Del Rey, California 90292, U.S.A.               #
#                                                                            #
# Permission is hereby granted, free of charge, to any person obtaining      #
# a copy of this software and associated documentation files (the            #
# "Software"), to deal in the Software without restriction, including        #
# without limitation the rights to use, copy, modify, merge, publish,        #
# distribute, sublicense, and/or sell copies of the Software, and to         #
# permit persons to whom the Software is furnished to do so, subject to      #
# the following conditions:                                                  #
#                                                                            #
# The above copyright notice and this permission notice shall be             #
# included in all copies or substantial portions of the Software.            #
#                                                                            #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,            #
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF         #
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND                      #
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE     #
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION     #
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION      #
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.            #
#                                                                            #
############################# END LICENSE BLOCK ##############################


import sys
import os
import os.path
from io import StringIO

import pandas

import kgtk.stetools.api as kpi


class PandasReader(object):

    def __init__(self, infile=None, logfile=None, chunk_size=1000000):
        self.iter = kpi.pkgtk.allocateKgtkValidationIterator(infile, logfile)
        self.iter.chunkSize = chunk_size
        self.header = None
        self.nrows = 0

    def __iter__(self):
        return self

    def next(self):
        if self.iter.nextP():
            self.header = self.header or list(map(str, self.iter.headerColumns))
            value = self.iter.value.theString()
            df = pandas.read_csv(StringIO(value), engine='c', sep='\t', quoting=3, header=None, names=self.header, dtype=str, na_values='')
            self.nrows += len(df)
            return df
        else:
            self.iter.close()
            raise StopIteration()
        
    def __len__(self):
        return len(self.iter)


def testPandasReader1(file, logFile=None, chunkSize=10000):
    # Run the validation, copy the string and create a Pandas dataframe for it.
    iter = kpi.pkgtk.allocateKgtkValidationIterator(file, logFile)
    iter.chunkSize = chunkSize
    header = None
    nTotalItems = 0
    frame1 = None
    frameN = None
    for value in iter:
        header = header or list(map(str, iter.headerColumns))
        value = value.theString()
        # supplying verbose=True shows that tokenization takes about 25% and value conversion 75% of the time;
        # forcing dtype to str doesn't help much, setting na_filter=False saves 3% but leaves us without NaNs:
        df = pandas.read_csv(StringIO(value), engine='c', sep='\t', quoting=3, header=None, names=header, dtype=str, na_values='')
        nTotalItems += len(df) * df.shape[1]
        if frame1 is None:
            frame1 = df
        frameN = df
    iter.close()
    iter.frame1 = frame1
    iter.frameN = frameN
    return iter.lineNumber, nTotalItems, iter


"""
>>> import kgtk.stetools.pandasreader as pr

>>> with Timer() as t:
...     pr.testPandasReader1("/data/kgtk/wikidata/run2/wikidata_edges_20200330-slice.tsv", "/tmp/errors.log", chunkSize=500000)
(1000000, 12999987, <proxy.KgtkValidationIterator |i|/KGTK/@KGTK-VALIDATION-ITERATOR>)
>>> t.elapsed
1.6856780052185059
>>> 

# using pandas gets us to about 30m+ for the full 1B edge file, chunkSize=1M gives us about a 1GB process footprint:
>>> with Timer() as t:
...     pr.testPandasReader1("/data/kgtk/wikidata/run2/wikidata_edges_20200330-slice-100M.tsv", "/tmp/errors.log", chunkSize=1000000)
... 
(100000000, 1299999987, <proxy.KgtkValidationIterator |i|/KGTK/@KGTK-VALIDATION-ITERATOR>)
>>> iter = _[2]
>>> t.elapsed
188.92706298828125
>>> iter.frame1
                     id    node1  label                                              node2  ... longitude            precision  calendar entity-type
0            Q8-P1245-1       Q8  P1245                                           "885155"  ...       NaN                  NaN       NaN         NaN
1             Q8-P373-1       Q8   P373                                        "Happiness"  ...       NaN                  NaN       NaN         NaN
2              Q8-P31-1       Q8    P31                                            Q331769  ...       NaN                  NaN       NaN        item
3              Q8-P31-2       Q8    P31                                          Q60539479  ...       NaN                  NaN       NaN        item
4              Q8-P31-3       Q8    P31                                              Q9415  ...       NaN                  NaN       NaN        item
...                 ...      ...    ...                                                ...  ...       ...                  ...       ...         ...
999994  Q705912-P1619-1  Q705912  P1619                           ^1999-06-30T00:00:00Z/11  ...       NaN                   11  Q1985727         NaN
999995   Q705912-P373-1  Q705912   P373                               "Naengjeong Station"  ...       NaN                  NaN       NaN         NaN
999996   Q705912-P625-1  Q705912   P625                                  @35.1513/129.0124  ...  129.0124  2.7777777777778e-06       NaN         NaN
999997   Q705912-P296-1  Q705912   P296                                              "224"  ...       NaN                  NaN       NaN         NaN
999998   Q705928-P373-1  Q705928   P373                           "Typhoon Jelawat (2012)"  ...       NaN                  NaN       NaN         NaN

[999999 rows x 13 columns]
>>> iter.frameN
                        id      node1  label                                              node2  ... longitude precision  calendar entity-type
0        Q51124948-P2093-2  Q51124948  P2093                                            "Ryu M"  ...       NaN       NaN       NaN         NaN
1        Q51124948-P2093-3  Q51124948  P2093                                        "Przypek J"  ...       NaN       NaN       NaN         NaN
2          Q51124948-P31-1  Q51124948    P31                                          Q13442814  ...       NaN       NaN       NaN        item
3         Q51124948-P698-1  Q51124948   P698                                          "7610129"  ...       NaN       NaN       NaN         NaN
4        Q51124948-P1433-1  Q51124948  P1433                                           Q3618986  ...       NaN       NaN       NaN        item
...                    ...        ...    ...                                                ...  ...       ...       ...       ...         ...
999995    Q52247714-P478-1  Q52247714   P478                                                "3"  ...       NaN       NaN       NaN         NaN
999996   Q52247714-P1476-1  Q52247714  P1476  'Difference in scopolamine sensitivity during ...  ...       NaN       NaN       NaN         NaN
999997   Q52247714-P1433-1  Q52247714  P1433                                           Q6295819  ...       NaN       NaN       NaN        item
999998    Q52247714-P304-1  Q52247714   P304                                          "142-148"  ...       NaN       NaN       NaN         NaN
999999    Q52247714-P577-1  Q52247714   P577                           ^1989-01-01T00:00:00Z/11  ...       NaN        11  Q1985727         NaN

[1000000 rows x 13 columns]
>>> 

# selecting all ?i P31 ?c tuples (6M lines from 100M tuples):
>>> with Timer() as t:
...     need_header = True
...     for df in pr.PandasReader("/data/kgtk/wikidata/run2/wikidata_edges_20200330-slice-100M.tsv"):
...         df = df[df.label == 'P31'][['node1', 'label', 'node2']]
...         df.to_csv('/tmp/p31-via-pandas-reader.tsv', header=need_header, index=None, sep='\t', mode='a')
...         need_header = False
Line 222620[4]: Bad syntax: "Ca₂(Mg,Fe)₅[OH|
Line 270163[4]: Bad syntax: "170033|
Line 348068[4]: Bad syntax: "a,b \in \mathbb{Z} \land b \neq 0 \implies \exists! q,r \mid a=bq+r \land 0 \leq r < |
   ..............
Line 96517665[4]: Bad syntax: 'Maquette au 1/100e du sous-marin français [[:fr:Rubis (1931)|
Line 97510851[4]: Bad syntax: 'Computational Exploration of the Li-Electrode|
Line 99067742[4]: Bad syntax: "55B8-55B8|
>>> t.elapsed
213.4009280204773
>>> 
"""
