These datasets were used in a series of matching experiments with the
WHIRL system.  I'm providing them as a resource for other people
interested in matching and clustering entity names.

==============================================================================
Description
==============================================================================

The data here is broken up by domain.  There are eight different
"domains", each of which contains a different type of entity name (eg
animal names, bird names, business names, etc).  Six of these domains
have been experimented on with WHIRL, and two have not (these came to
me from Nick Kushmeric, via Ray Mooney).  

I'm aware of a couple of additional matching/clustering datasets that
are available (the CORA dataset, and one additional Whirl dataset)
which I plan to put up here later.  Any other contributions will be
happily accepted.

The simplest format to recover from is probably the *.txt files.  These files
contain tab-delimited text fields such that:
 - field 1 is the name of the relation from which this example comes
 - field 2 is (usually) an ID value, which can be used to assess the correctness of a match
 - the remaining fields are text fields that identify an entity. usually there's only one. 

The experiments I propose are to do a soft join across every pair of
relations contained in a single file. The exception is relation pairs
named foo and foox, which are always variants of each other, in which
entity names in "foox" are like "foo" except that any additional
"noise" text has been removed.  

In addition to this I'm including the original versions of the files
that I experimented on, in the subdirectories labeled "orig".  In
these versions:
 - the data is in XML format in a file foo.stir (this is also viewable
 in a browser as an XHTML table, BTW)
 - the scripts used by WHIRL to join the data are in files foo.whirl
 - the output of WHIRL's join, as an HTML table, with "correct"
 matches highlighted in green, is in foo.html
 - a gnuplot-formatted precision-recall curve for WHIRL's matching
 performance is in foo.gdata.  The first line of foo.gdata is a
 comment giving whirl's average precision on the data.

==============================================================================
What's here:
==============================================================================

animal/animal.txt
	two relations: park_animals and endangered_species.
	there is no key for these relations - instead, a match is considered
	correct if the tokens in either scientific name are a proper
	subset of the other.
birds/kunkel.txt
	two relations: map1 and map2.  keys are URLs
birds/scott1.txt
	two relations: cscott and dsarkimg.  keys are URLS
birds/scott2.txt
	two relations: bscott and mbrimg.  keys are URLS
birds/nybird.txt
	three relations: nabird, call, callx.  keys are scientific names.
	the "entity names" (birds) in call are embedded in additional text.
	in callx, this additional text has been (manually) removed.
business/business.txt
	two relations: hooverweb and iontech.  keys are top-level URLs.
	roughly 10-15% of the URLs don't match when they should.
game-demos/demos.txt
	three relations: demo, demox, and newsweek.  the keys are hand-generated.
	the "entity names" in demo are embedded in additional text.
	in demox, this additional text has been (manually) removed.
parks/parks.txt
	two relations: npspark and icepark.  keys are URLs, which seem to be
	much less noisy for this case than for the business domain.
restaurant/restaurant.txt
	two relations: f and z (Fodor's and Zagrat's, respectively).
	Originally from Sheila Tejada at ISI.  Keys are phone numbers,
	manually edited to reflect the few cases in which phone
	numbers are not correct keys.
ucb-people/ucb-people.txt
	three relations: ucb1 and ucb2, plus an additional "unknown" relation.
	probably ucb1 and ucb2 contain all the matches from two original datasets
	D1 and D2, and the "unknown" dataset contains the non-matches
	from both datasets. source: Nick Kushmeric at UC/Dublin.
vauniv/vauniv.txt
	one relation: vauniv (hence this is a clustering problem,
	where the ids are desired clusters, not a matching problem).
