This is the code for the Find the Expert app located at http://findtheexpertapp.com.

It is a first iteration of the project. In order to run it, you will need:
 * An unzipped copy of the Stack Overflow data dump. A bittorrent file is included here to facilitate your download. The project uses the August dump; a new dump is released every three months. Warning: it's 7GB!
 * A running copy of MySQL. You will need to create a new user and database. The code assumes it will not have root access,
   so this step must be done earlier. 

You will also need some additional non-standard Python packages, including:
 * Numeric Python, numpy
 * Scientific Python, scipy
 * Beautiful Soup
 * gensim (http://radimrehurek.com/gensim)

To run:

 1. Create a MySQL database. 
 1. Create a MySQL user and grant it full rights to the database.
 1. Copy config.py.tmpl to config.py
 1. Edit config.py in your favorite text editor. Put in your MySQL database and login information.
 1. Import the stack overflow dump into the database by running sov2mysql.py. It will take some time.
 1. Run the indexing application to index the set of tags that you want indexed: topic\_classification.py tags; Note that indexing more tags takes more memory. My 8 GB RAM machine could not handle more than 100,000 posts effectively. Index creation can be distributed if more machines are available, but one machine will require a lot of memory to hold the full matrix. Alternatively, the number of topics may be reduced or (ideally) stopwords may be chosen more carefully to reduce the number of features. 
 1. Run comment\_classification.py (which uses the trained classifier in comment.classifier)
 1. Run controller.py: a server should run at http://localhost:5000 (unless you changed the port in config.py).  

If, at any time you need to reset from the start: in mySQL, remove and recreate the database, and remove all of the corpus index files.

License:

Unless otherwise specified in the source, this code is released under the BSD License:

Copyright 2012 Efraim Feinstein

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 1. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
