# based on:
# https://docs.google.com/spreadsheets/d/1xvLcqCZ-fVhkOBkH3E3wpuR4h3VOtWFdO9z3atRPK9k/edit#gid=1389523714

queries = {
    'Methane Removal': [
        {'qid': 'c_18', 'query': '"methane direct air capture"'},
        {'qid': 'c_19', 'query': '"methane capture"'},
        {'qid': 'c_54', 'query': 'methane removing atmosphere'},
    ],
    'CCS': [
        {'qid': 'c_10', 'query': '"co2 sequestration" storage'},
        {'qid': 'c_11', 'query': '"carbon sequestration" storage'},
        {'qid': 'c_12', 'query': '"carbon dioxide sequestration"'},
        {'qid': 'c_13', 'query': '"carbon capture" storage'},
        {'qid': 'c_14', 'query': '"carbon storage" capture'},
        {'qid': 'c_15', 'query': '"carbon dioxide capture" storage'},
        {'qid': 'c_16', 'query': '"carbon dioxide storage" capture'},
        {'qid': 'c_17', 'query': 'CCS (climate OR carbon OR co2)'},
    ],
    'Ocean Fertilization': [
        {'qid': 'c_20', 'query': '"ocean fertilization" OR "ocean fertilisation" '},
        {'qid': 'c_21', 'query': '"iron fertilization" OR "iron fertilisation"'},
        {'qid': 'c_22',
         'query': '(fertilization OR fertilisation) (phytoplankton OR algae) (climate OR carbon OR co2)'},
        {'qid': 'c_50', 'query': '"iron seeding" (climate OR co2 OR carbon)'},
    ],
    'Ocean Alkalinization': [
        {'qid': 'c_23', 'query': '"ocean liming" -from:spangletoes'},
        {'qid': 'c_49', 'query': '"ocean alkalinity enhancement"'},
        {'qid': 'c_57', 'query': '"ocean alkalinization" OR "ocean alkalinisation"'},
    ],
    'Enhanced Weathering': [
        {'qid': 'c_24', 'query': '"enhanced weathering" -from:spangletoes'},
        {'qid': 'c_25', 'query': '(olivine OR basalt OR silicate) (co2 OR emission OR emissions)'},
        {'qid': 'c_26', 'query': 'olivine weathering'},
        {'qid': 'c_51', 'query': '(basalt OR silicate) weathering (co2 OR carbon OR enhanced)'},
    ],
    'Biochar': [
        {'qid': 'c_27',
         'query': '(biochar OR bio-char) (co2 OR carbon OR climate OR emission OR sequestration OR "greenhouse gas")'},
    ],
    'Afforestation/Reforestation': [
        {'qid': 'c_29',
         'query': 'afforestation (climate OR co2 OR emission OR emissions OR  "greenhouse gas"  OR ghg OR carbon)'},
        {'qid': 'c_30',
         'query': 'reforestation (climate OR co2 OR emission OR emissions OR  "greenhouse gas"  OR ghg OR carbon)'},
        {'qid': 'c_31', 'query': 'tree planting climate'}
    ],
    'Ecosystem Restoration': [
        {'qid': 'c_32', 'query': '(re-wilding OR rewilding) (climate OR carbon OR CO2 OR "greenhouse gas" OR GHG)'},
        {'qid': 'c_56',
         'query': '("ecosystem restoration" OR "restore ecosystem")  (climate OR carbon OR CO2 OR "greenhouse gas" OR GHG)'},
    ],
    'Soil Carbon Sequestration': [
        {'qid': 'c_33', 'query': 'soil sequestration (co2 OR carbon)'},
        {'qid': 'c_36', 'query': '"soil carbon"'},
        {'qid': 'c_37', 'query': '"carbon farming"'},
    ],
    'BECCS': [
        {'qid': 'c_38',
         'query': 'BECCS (co2 OR carbon OR climate OR ccs OR biomass OR emission OR emissions)'},
        {'qid': 'c_39',
         'query': 'biomass ("carbon capture" OR "capture carbon" OR  "co2 capture" OR "capture CO2" OR ccs)'},
        {'qid': 'c_40',
         'query': 'bioenergy ("carbon capture" OR "capture carbon" OR  "co2 capture" OR "capture CO2" OR ccs)'},
    ],
    'Blue Carbon': [
        {'qid': 'c_41', 'query': 'seagrass (carbon OR co2)'},
        {'qid': 'c_42', 'query': 'macroalgae (carbon OR co2)'},
        {'qid': 'c_43', 'query': 'mangrove (carbon OR co2)'},
        {'qid': 'c_52', 'query': 'kelp (carbon OR co2)'},
        {'qid': 'c_53',
         'query': '(wetland OR wetlands OR marsh OR marshes OR peatland OR peatlands OR peat OR bog OR  bogs) (carbon OR co2) (restore OR restoration OR rehabilitation)'},
        {'qid': 'c_44', 'query': '"blue carbon"'},
    ],
    'Direct Air Capture': [
        {'qid': 'c_45', 'query': 'DAC (climate OR carbon OR co2 OR emission OR emissions)'},
        {'qid': 'c_46', 'query': '"direct air capture"'},
        {'qid': 'c_47', 'query': '("carbon capture" OR "co2 capture") ("ambient air" OR "direct air")'},
        {'qid': 'c_48', 'query': 'DACCS (carbon OR co2 OR climate)'},
    ],
    'GGR (general)': [
        {'qid': 'c_09', 'query': '"methane removal"'},
        {'qid': 'c_01', 'query': '"negative emissions"'},
        {'qid': 'c_02', 'query': '"negative emission"'},
        {'qid': 'c_03', 'query': '"carbon dioxide removal"'},
        {'qid': 'c_04', 'query': '"co2 removal" -submarine -"space station"'},
        {'qid': 'c_05', 'query': '"carbon removal"'},
        {'qid': 'c_06', 'query': '"greenhouse gas removal"'},
        {'qid': 'c_07', 'query': '"ghg removal"'},
        {'qid': 'c_08', 'query': '"carbon negative" (climate OR co2 OR emission OR  "greenhouse gas"  OR ghg)'},
        {'qid': 'c_55', 'query': '(remove OR removing OR removed) (carbon OR co2) atmosphere'},
    ]
}
