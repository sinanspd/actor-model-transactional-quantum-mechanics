My attempt at loosely modelling transactional interpration of quantum mechanics using actors. 


This interpreation is rather ambigious (a sentiment I found shared by Peter Shor). It remains unclear to me in what cases multiple emitters are appropiate. 

1) In a direct mapping, multiple emmitters can correspond to multiple physical (photon) sources. 
2) One emission with multiple coherant sub-sources, similar to the double slit experiment (where each slit is a emitter)

It is important to realize that quantum computing is more restrictive than quantum mechanics (understandably). Even though this code supports multiple emitters, it is unlikely that in the circuit model, we would explicility identify multiple emitters. The evolution of a single state is roughly multiple emitters whose waves interfere is the superposition of distinct paths contributing to the same source. On the other hand, it is clear that multiple receivers correspond to orthogonal measurement outcomes. 