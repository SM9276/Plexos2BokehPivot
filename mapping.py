import os
import sys
import clr
import pandas as pd
import json
import re

sys.path.append('C:/Program Files/Energy Exemplar/PLEXOS 10.0 API')
clr.AddReference('PLEXOS_NET.Core')
clr.AddReference('EEUTILITY')
clr.AddReference('EnergyExemplar.PLEXOS.Utility')

from PLEXOS_NET.Core import DatabaseCore
from EnergyExemplar.PLEXOS.Utility.Enums import *
from EEUTILITY.Enums import *

def recordset_to_list(rs):
    result = []
    while not rs.EOF:
        row = [rs[f.Name] for f in rs.Fields]
        result.append(row)
        rs.MoveNext()
    return result

def parse_collection_enum(collection_enum_str):
    """
    Parse the CollectionEnum string into a dictionary mapping collection_id to collection_name.
    
    Args:
    - collection_enum_str: The string containing the CollectionEnum definitions.
    
    Returns:
    - dict: A dictionary where keys are collection_ids and values are collection_names.
    """
    collection_mapping = {}
    lines = collection_enum_str.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line or line.startswith('CollectionEnum') or line.startswith('System.'):
            continue
        match = re.match(r'(\w+)\s*=\s*(\d+)', line)
        if match:
            name, cid = match.groups()
            collection_mapping[int(cid)] = name
    return collection_mapping

def get_report_properties(model_name):
    """
    Get a DataFrame containing combined report and property information based on the specified model's reports.
    
    Args:
    - model_name: The name of the model to retrieve report properties for.
    
    Returns:
    - DataFrame: Combined DataFrame of report and property information.
    """
    # Load the XML model file
    model = DatabaseCore()
    model.Connection(os.path.join(os.path.dirname(__file__), 'testmodel.xml'))
    
    # Extract class and collection IDs
    classes = model.FetchAllClassIds()
    collections = model.FetchAllCollectionIds()

    # Extract Report IDs
    report_names = model.GetChildMembers(collections["ModelReport"], model_name)
    report_ids = [model.ObjectName2Id(classes["Report"], name) for name in report_names]
    
    # Use the GetData function to get a record set with t_report into a pd data frame
    recordset, _ = model.GetData('t_report', [])
    report_data = recordset_to_list(recordset)
    report_df = pd.DataFrame(report_data, columns=[f.Name for f in recordset.Fields])
    recordset.Close()
    
    # Filter to include only reports matching the model
    report_df['filtered_report'] = report_df['object_id'].apply(lambda x: x in report_ids)
    report_df = report_df[report_df['object_id'].isin(report_ids)]
    
    # Use the GetData function to get a record set with t_property_report into a pd data frame
    recordset, _ = model.GetData('t_property_report', [])
    report_prop_data = recordset_to_list(recordset)
    report_prop_df = pd.DataFrame(report_prop_data, columns=[f.Name for f in recordset.Fields])
    recordset.Close()

    # Set index and join the DataFrames
    report_prop_df = report_prop_df.set_index('property_id')
    combined_df = report_df.join(report_prop_df, on='property_id', rsuffix='_prop')

    # Close the model file
    model.Close()
    return combined_df

def create_mapping(df, collection_mapping):
    """
    Create a mapping of collection_ids to selected properties based on user input.
    
    Args:
    - df: DataFrame containing the combined report and property information.
    - collection_mapping: Dictionary mapping collection_id to collection_name.
    
    Returns:
    - None. The mapping is saved to 'mapping.json'.
    """
    # Get unique collection_ids
    collection_ids = df['collection_id'].unique()
    
    # Map collection_ids to collection names using the collection_mapping
    collection_map = {}
    for cid in collection_ids:
        cname = collection_mapping.get(cid, f"Collection {cid}")
        collection_map[cid] = cname
    
    # Display the collection_ids to the user
    print("Available Collections:")
    for idx, (cid, cname) in enumerate(collection_map.items()):
        print(f"{idx + 1}. {cname} (ID: {cid})")
    
    # Ask the user to select collection_ids
    selected_collections = input("\nEnter the numbers of the collections you want to select (e.g., 1,3,5): ")
    selected_indices = [int(x.strip()) - 1 for x in selected_collections.split(',')]
    selected_collection_ids = [list(collection_map.keys())[i] for i in selected_indices]
    
    mapping = {}
    # For each selected collection_id, let the user select properties
    for cid in selected_collection_ids:
        cname = collection_map[cid]
        # Get properties associated with this collection_id
        prop_df = df[df['collection_id'] == cid]
        properties = prop_df[['property_id', 'name']].drop_duplicates()
        property_ids = properties['property_id'].unique()
        # Map property_ids to property names for display
        property_map = {}
        for idx, row in properties.iterrows():
            pid = row['property_id']
            pname = row['name']
            property_map[pid] = pname
        # Display properties to the user
        print(f"\nProperties for {cname} (Collection ID: {cid}):")
        for idx, (pid, pname) in enumerate(property_map.items()):
            print(f"{idx + 1}. {pname} (Property ID: {pid})")
        # Ask the user to select properties
        selected_props = input(f"Enter the numbers of the properties you want to select for '{cname}' (e.g., 1,3,5): ")
        selected_prop_indices = [int(x.strip()) - 1 for x in selected_props.split(',')]
        selected_property_ids = [list(property_map.keys())[i] for i in selected_prop_indices]
        # Save to mapping
        mapping[str(cid)] = selected_property_ids  # Use str(cid) to make JSON keys strings
    
    # Save the mapping to mapping.json
    mapping_path = os.path.join(os.path.dirname(__file__), 'mapping.json')
    with open(mapping_path, 'w') as f:
        json.dump(mapping, f, indent=4)
    
    print(f"\nMapping saved to {mapping_path}")

def main():
    model_name = 'RPS_000_SPS_000'
    df = get_report_properties(model_name)
    
    # The CollectionEnum string provided
    collection_enum_str = '''
    CollectionEnum
        SystemGenerators = 1
        GeneratorTemplate = 2
        ListGenerators = 3
        GeneratorHeatInput = 4
        GeneratorTransition = 5
        GeneratorPowerStation = 6
        GeneratorFuels = 7
        GeneratorStartFuels = 8
        GeneratorSourcePower2X = 9
        GeneratorHeadStorage = 10
        GeneratorTailStorage = 11
        GeneratorNodes = 12
        GeneratorNodes_star_ = 13
        GeneratorHeatInputNodes = 14
        GeneratorHeatOutputNodes = 15
        GeneratorSourceGasFields = 16
        GeneratorSourceGasPlants = 17
        GeneratorGasNode = 18
        GeneratorStartGasNodes = 19
        GeneratorSourceGasStorages = 20
        GeneratorSourceGasContracts = 21
        GeneratorSourceGasTransports = 22
        GeneratorWaterNode = 23
        GeneratorCompanies = 24
        GeneratorCommoditiesConsumed = 25
        GeneratorCommoditiesProduced = 26
        GeneratorMaintenances = 27
        GeneratorFlowNodes = 28
        GeneratorCapacityMarkets = 29
        GeneratorHeatMarkets = 30
        GeneratorMarktoMarkets = 31
        GeneratorConstraints = 32
        GeneratorObjectives = 33
        GeneratorDecisionVariables = 34
        GeneratorConditions = 35
        SystemPowerStations = 36
        PowerStationTemplate = 37
        ListPowerStations = 38
        PowerStationNodes = 39
        SystemFuels = 40
        FuelTemplate = 41
        ListFuels = 42
        FuelSourcePower2X = 43
        FuelSourceGasFields = 44
        FuelSourceGasPlants = 45
        FuelGasNodes = 46
        FuelSourceGasStorages = 47
        FuelSourceGasContracts = 48
        FuelSourceGasTransports = 49
        FuelCompanies = 50
        FuelFacilities = 51
        FuelFlowNodes = 52
        FuelMarkets = 53
        FuelConstraints = 54
        FuelObjectives = 55
        FuelConditions = 56
        SystemFuelContracts = 57
        FuelContractTemplate = 58
        ListFuelContracts = 59
        FuelContractGenerators = 60
        FuelContractFuel = 61
        FuelContractCompanies = 62
        FuelContractConstraints = 63
        FuelContractObjectives = 64
        SystemPower2X = 65
        Power2XTemplate = 66
        ListPower2X = 67
        Power2XFuels = 68
        Power2XNodes = 69
        Power2XHeatNodes = 70
        Power2XHeatStorages = 71
        Power2XGasNodes = 72
        Power2XGasStorages = 73
        Power2XWaterNodes = 74
        Power2XCompanies = 75
        Power2XCommodities = 76
        Power2XFlowNodes = 77
        Power2XConstraints = 78
        Power2XObjectives = 79
        SystemBatteries = 80
        BatteryTemplate = 81
        ListBatteries = 82
        BatteryNodes = 83
        BatteryNodes_star_ = 84
        BatteryCompanies = 85
        BatteryCommoditiesConsumed = 86
        BatteryCommoditiesProduced = 87
        BatteryFlowNodes = 88
        BatteryCapacityMarkets = 89
        BatteryConstraints = 90
        BatteryObjectives = 91
        BatteryConditions = 92
        SystemStorages = 93
        StorageTemplate = 94
        ListStorages = 95
        StorageWaterNodes = 96
        StorageConstraints = 97
        StorageObjectives = 98
        StorageConditions = 99
        StorageGlobals = 100
        SystemWaterways = 101
        WaterwayTemplate = 102
        ListWaterways = 103
        WaterwayStorageFrom = 104
        WaterwayStorageTo = 105
        WaterwayConstraints = 106
        WaterwayObjectives = 107
        SystemEmissions = 108
        EmissionTemplate = 109
        ListEmissions = 110
        EmissionGenerators = 111
        EmissionFuels = 112
        EmissionPower2X = 113
        EmissionGasFields = 114
        EmissionGasPlants = 115
        EmissionGasNodes = 116
        EmissionGasDemands = 117
        EmissionGasContracts = 118
        EmissionGasTransports = 119
        EmissionWaterPlants = 120
        EmissionVehicles = 121
        EmissionCommodities = 122
        EmissionFacilities = 123
        EmissionMarkets = 124
        EmissionConstraints = 125
        EmissionObjectives = 126
        SystemAbatements = 127
        AbatementTemplate = 128
        ListAbatements = 129
        AbatementGenerators = 130
        AbatementConsumables = 131
        AbatementEmissions = 132
        AbatementGasFields = 133
        AbatementGasPlants = 134
        AbatementGasNodes = 135
        AbatementGasDemands = 136
        AbatementGasContracts = 137
        AbatementConstraints = 138
        AbatementObjectives = 139
        SystemPhysicalContracts = 140
        PhysicalContractTemplate = 141
        ListPhysicalContracts = 142
        PhysicalContractGenerationNode = 143
        PhysicalContractLoadNode = 144
        PhysicalContractCompanies = 145
        PhysicalContractConstraints = 146
        PhysicalContractObjectives = 147
        SystemPurchasers = 148
        PurchaserTemplate = 149
        ListPurchasers = 150
        PurchaserNodes = 151
        PurchaserNodes_star_ = 152
        PurchaserCompanies = 153
        PurchaserConstraints = 154
        PurchaserObjectives = 155
        SystemReserves = 156
        ReserveTemplate = 157
        ListReserves = 158
        ReserveGenerators = 159
        ReserveGeneratorContingencies = 160
        ReserveGeneratorCostAllocation = 161
        ReservePower2X = 162
        ReserveBatteries = 163
        ReserveBatteryContingencies = 164
        ReservePurchasers = 165
        ReserveNestedReserves = 166
        ReserveRegions = 167
        ReserveZones = 168
        ReserveLines = 169
        ReserveLineContingencies = 170
        ReserveMarkets = 171
        ReserveConstraints = 172
        ReserveObjectives = 173
        ReserveConditions = 174
        SystemReliability = 175
        ReliabilityTemplate = 176
        ListReliability = 177
        ReliabilityGenerators = 178
        ReliabilityRegion = 179
        SystemFinancialContracts = 180
        FinancialContractTemplate = 181
        ListFinancialContracts = 182
        FinancialContractGenerators = 183
        FinancialContractRegion = 184
        FinancialContractRegions = 185
        FinancialContractGeneratingCompanies = 186
        FinancialContractPurchasingCompanies = 187
        FinancialContractConditions = 188
        SystemCournots = 189
        CournotTemplate = 190
        ListCournots = 191
        CournotRegion = 192
        SystemRSIs = 193
        RSITemplate = 194
        ListRSIs = 195
        RSIRegion = 196
        RSILines = 197
        RSIInterfaces = 198
        RSICompanies = 199
        SystemRegions = 200
        RegionTemplate = 201
        ListRegions = 202
        RegionGenerators = 203
        RegionBatteries = 204
        RegionEmissions = 205
        RegionGenerationContracts = 206
        RegionLoadContracts = 207
        RegionPurchasers = 208
        RegionRegions = 209
        RegionPool = 210
        RegionReferenceNode = 211
        RegionExportingLines = 212
        RegionImportingLines = 213
        RegionInterregionalLines = 214
        RegionIntraregionalLines = 215
        RegionExportingTransformers = 216
        RegionImportingTransformers = 217
        RegionInterregionalTransformers = 218
        RegionIntraregionalTransformers = 219
        RegionHeatPlants = 220
        RegionGasPlants = 221
        RegionGasStorages = 222
        RegionWaterPlants = 223
        RegionUtilities = 224
        RegionFacilities = 225
        RegionMarkets = 226
        RegionConstraints = 227
        RegionObjectives = 228
        RegionConditions = 229
        SystemPools = 230
        PoolTemplate = 231
        ListPools = 232
        PoolORDCReserves = 233
        PoolPools = 234
        PoolORDCSystemLambdaNodes = 235
        PoolCompanies = 236
        SystemZones = 237
        ZoneTemplate = 238
        ListZones = 239
        ZoneGenerators = 240
        ZoneCapacityGenerators = 241
        ZoneBatteries = 242
        ZoneCapacityBatteries = 243
        ZoneEmissions = 244
        ZoneCapacityGenerationContracts = 245
        ZoneCapacityLoadContracts = 246
        ZoneGenerationContracts = 247
        ZoneLoadContracts = 248
        ZonePurchasers = 249
        ZoneCapacityPurchasers = 250
        ZoneRegion = 251
        ZoneZones = 252
        ZoneReferenceNode = 253
        ZoneExportingCapacityLines = 254
        ZoneExportingLines = 255
        ZoneImportingCapacityLines = 256
        ZoneImportingLines = 257
        ZoneInterzonalLines = 258
        ZoneIntrazonalLines = 259
        ZoneExportingCapacityTransformers = 260
        ZoneExportingTransformers = 261
        ZoneImportingCapacityTransformers = 262
        ZoneImportingTransformers = 263
        ZoneInterzonalTransformers = 264
        ZoneIntrazonalTransformers = 265
        ZoneHeatPlants = 266
        ZoneCapacityHeatPlants = 267
        ZoneGasPlants = 268
        ZoneCapacityGasPlants = 269
        ZoneGasStorages = 270
        ZoneCapacityGasStorages = 271
        ZoneWaterPlants = 272
        ZoneCapacityWaterPlants = 273
        ZoneFacilities = 274
        ZoneCapacityFacilities = 275
        ZoneMarkets = 276
        ZoneCapacityMarkets = 277
        ZoneConstraints = 278
        ZoneObjectives = 279
        ZoneConditions = 280
        SystemNodes = 281
        NodeTemplate = 282
        ListNodes = 283
        NodeRegion = 284
        NodeZone = 285
        NodeCapacityZones = 286
        NodeHubs = 287
        NodeCompanies = 288
        NodeFacilities = 289
        NodeExportingFlowPaths = 290
        NodeImportingFlowPaths = 291
        NodeMarkets = 292
        NodeConstraints = 293
        NodeObjectives = 294
        NodeDecisionVariables = 295
        NodeConditions = 296
        NodeWeatherStations = 297
        SystemLoads = 298
        LoadTemplate = 299
        ListLoads = 300
        LoadNode = 301
        LoadCompany = 302
        SystemLines = 303
        LineTemplate = 304
        ListLines = 305
        LineNodeFrom = 306
        LineNodeTo = 307
        LineCompanies = 308
        LineMaintenances = 309
        LineConstraints = 310
        LineObjectives = 311
        LineConditions = 312
        SystemMLFs = 313
        MLFTemplate = 314
        ListMLFs = 315
        MLFRegions = 316
        MLFNode = 317
        MLFLine = 318
        SystemTransformers = 319
        TransformerTemplate = 320
        ListTransformers = 321
        TransformerNodeFrom = 322
        TransformerNodeTo = 323
        TransformerConstraints = 324
        TransformerObjectives = 325
        SystemFlowControls = 326
        FlowControlTemplate = 327
        ListFlowControls = 328
        FlowControlLine = 329
        FlowControlLines_star_ = 330
        FlowControlConstraints = 331
        FlowControlObjectives = 332
        SystemInterfaces = 333
        InterfaceTemplate = 334
        ListInterfaces = 335
        InterfaceLines = 336
        InterfaceTransformers = 337
        InterfaceConstraints = 338
        InterfaceObjectives = 339
        InterfaceConditions = 340
        SystemContingencies = 341
        ContingencyTemplate = 342
        ListContingencies = 343
        ContingencyGenerators = 344
        ContingencyLines = 345
        ContingencyLinesMonitored = 346
        ContingencyMonitoredLines = 347
        ContingencyTransformers = 348
        ContingencyMonitoredTransformers = 349
        ContingencyTransformersMonitored = 350
        ContingencyInterfacesMonitored = 351
        ContingencyMonitoredInterfaces = 352
        SystemHubs = 353
        HubTemplate = 354
        ListHubs = 355
        HubConstraints = 356
        HubObjectives = 357
        SystemTransmissionRights = 358
        TransmissionRightTemplate = 359
        ListTransmissionRights = 360
        TransmissionRightZoneFrom = 361
        TransmissionRightZoneTo = 362
        TransmissionRightNodeFrom = 363
        TransmissionRightNodeTo = 364
        TransmissionRightLine = 365
        TransmissionRightHubFrom = 366
        TransmissionRightHubTo = 367
        TransmissionRightCompanies = 368
        SystemHeatPlants = 369
        HeatPlantTemplate = 370
        ListHeatPlants = 371
        HeatPlantFuels = 372
        HeatPlantStartFuels = 373
        HeatPlantNodes = 374
        HeatPlantHeatInputNodes = 375
        HeatPlantHeatOutputNodes = 376
        HeatPlantGasNodes = 377
        HeatPlantConstraints = 378
        HeatPlantObjectives = 379
        HeatPlantConditions = 380
        SystemHeatNodes = 381
        HeatNodeTemplate = 382
        ListHeatNodes = 383
        HeatNodeHeatExportNodes = 384
        HeatNodeWaterPlants = 385
        HeatNodeFacilities = 386
        HeatNodeMarkets = 387
        HeatNodeConstraints = 388
        HeatNodeObjectives = 389
        HeatNodeConditions = 390
        SystemHeatStorages = 391
        HeatStorageTemplate = 392
        ListHeatStorages = 393
        HeatStorageHeatNodes = 394
        HeatStorageConstraints = 395
        HeatStorageObjectives = 396
        SystemGasFields = 397
        GasFieldTemplate = 398
        ListGasFields = 399
        GasFieldGasNode = 400
        GasFieldGasBasin = 401
        GasFieldCompanies = 402
        GasFieldMaintenances = 403
        GasFieldConstraints = 404
        GasFieldObjectives = 405
        SystemGasPlants = 406
        GasPlantTemplate = 407
        ListGasPlants = 408
        GasPlantNode = 409
        GasPlantInputNode = 410
        GasPlantOutputNode = 411
        GasPlantMaintenances = 412
        GasPlantConstraints = 413
        GasPlantObjectives = 414
        GasPlantDecisionVariables = 415
        SystemGasPipelines = 416
        GasPipelineTemplate = 417
        ListGasPipelines = 418
        GasPipelineGasNodeFrom = 419
        GasPipelineGasNodeTo = 420
        GasPipelineGasPaths = 421
        GasPipelineMaintenances = 422
        GasPipelineConstraints = 423
        GasPipelineObjectives = 424
        GasPipelineConditions = 425
        SystemGasNodes = 426
        GasNodeTemplate = 427
        ListGasNodes = 428
        GasNodeGasZones = 429
        GasNodeGasTransports = 430
        GasNodeGasPaths = 431
        GasNodeFacilities = 432
        GasNodeMarkets = 433
        GasNodeConstraints = 434
        GasNodeObjectives = 435
        SystemGasStorages = 436
        GasStorageTemplate = 437
        ListGasStorages = 438
        GasStorageSourcePower2X = 439
        GasStorageNode = 440
        GasStorageSourceGasFields = 441
        GasStorageSourceGasPlants = 442
        GasStorageGasNodes = 443
        GasStorageSourceGasStorages = 444
        GasStorageSourceGasContracts = 445
        GasStorageSourceGasTransports = 446
        GasStorageMaintenances = 447
        GasStorageConstraints = 448
        GasStorageObjectives = 449
        SystemGasDemands = 450
        GasDemandTemplate = 451
        ListGasDemands = 452
        GasDemandSourcePower2X = 453
        GasDemandSourceGasFields = 454
        GasDemandSourceGasPlants = 455
        GasDemandGasNodes = 456
        GasDemandSourceGasStorages = 457
        GasDemandLinkedGasContracts = 458
        GasDemandSourceGasContracts = 459
        GasDemandSourceGasTransports = 460
        GasDemandCompanies = 461
        SystemGasDSMPrograms = 462
        GasDSMProgramTemplate = 463
        ListGasDSMPrograms = 464
        GasDSMProgramGasDemands = 465
        GasDSMProgramConstraints = 466
        GasDSMProgramObjectives = 467
        SystemGasBasins = 468
        GasBasinTemplate = 469
        ListGasBasins = 470
        GasBasinConstraints = 471
        GasBasinObjectives = 472
        SystemGasZones = 473
        GasZoneTemplate = 474
        ListGasZones = 475
        GasZoneGenerators = 476
        GasZoneGasFields = 477
        GasZoneGasPlants = 478
        GasZoneExportingGasPipelines = 479
        GasZoneImportingGasPipelines = 480
        GasZoneInterzonalGasPipelines = 481
        GasZoneIntrazonalGasPipelines = 482
        GasZoneGasStorages = 483
        GasZoneGasDemands = 484
        GasZoneGasContracts = 485
        GasZoneExportingGasTransports = 486
        GasZoneImportingGasTransports = 487
        GasZoneInterzonalGasTransports = 488
        GasZoneIntrazonalGasTransports = 489
        SystemGasContracts = 490
        GasContractTemplate = 491
        ListGasContracts = 492
        GasContractGasFields = 493
        GasContractGasPipelines = 494
        GasContractGasNodes = 495
        GasContractGasTransports = 496
        GasContractGasPaths = 497
        GasContractCompanies = 498
        GasContractConstraints = 499
        GasContractObjectives = 500
    	SystemGasTransports = 501
        GasTransportTemplate = 502
        ListGasTransports = 503
        GasTransportSourcePower2X = 504
        GasTransportSourceGasFields = 505
        GasTransportSourceGasPlants = 506
        GasTransportExportNode = 507
        GasTransportImportNode = 508
        GasTransportSourceGasStorages = 509
        GasTransportSourceGasContracts = 510
        GasTransportSourceGasTransports = 511
        GasTransportGasPaths = 512
        GasTransportInitialGasPath = 513
        GasTransportMaintenances = 514
        GasTransportConstraints = 515
        GasTransportObjectives = 516
        SystemGasPaths = 517
        GasPathTemplate = 518
        ListGasPaths = 519
        SystemGasCapacityReleaseOffers = 520
        GasCapacityReleaseOfferTemplate = 521
        ListGasCapacityReleaseOffers = 522
        GasCapacityReleaseOfferGasPipelines = 523
        GasCapacityReleaseOfferGasStorages = 524
        GasCapacityReleaseOfferConstraints = 525
        GasCapacityReleaseOfferObjectives = 526
        SystemWaterPlants = 527
        WaterPlantTemplate = 528
        ListWaterPlants = 529
        WaterPlantNode = 530
        WaterPlantInputNode = 531
        WaterPlantOutputNode = 532
        WaterPlantMaintenances = 533
        WaterPlantConstraints = 534
        WaterPlantObjectives = 535
        WaterPlantDecisionVariables = 536
        SystemWaterPipelines = 537
        WaterPipelineTemplate = 538
        ListWaterPipelines = 539
        WaterPipelineWaterNodeFrom = 540
        WaterPipelineWaterNodeTo = 541
        WaterPipelineMaintenances = 542
        WaterPipelineConstraints = 543
        WaterPipelineObjectives = 544
        SystemWaterNodes = 545
        WaterNodeTemplate = 546
        ListWaterNodes = 547
        WaterNodeNode = 548
        WaterNodeWaterZones = 549
        WaterNodeFacilities = 550
        WaterNodeMarkets = 551
        WaterNodeConstraints = 552
        WaterNodeObjectives = 553
        SystemWaterStorages = 554
        WaterStorageTemplate = 555
        ListWaterStorages = 556
        WaterStorageWaterNode = 557
        WaterStorageMaintenances = 558
        WaterStorageConstraints = 559
        WaterStorageObjectives = 560
        SystemWaterDemands = 561
        WaterDemandTemplate = 562
        ListWaterDemands = 563
        WaterDemandWaterNodes = 564
        SystemWaterZones = 565
        WaterZoneTemplate = 566
        ListWaterZones = 567
        WaterZoneWaterPlants = 568
        WaterZoneExportingWaterPipelines = 569
        WaterZoneImportingWaterPipelines = 570
        WaterZoneInterzonalWaterPipelines = 571
        WaterZoneIntrazonalWaterPipelines = 572
        WaterZoneWaterStorages = 573
        WaterZoneWaterDemands = 574
        SystemWaterPumpStations = 575
        WaterPumpStationTemplate = 576
        ListWaterPumpStations = 577
        WaterPumpStationWaterPipeline = 578
        WaterPumpStationDownstreamWaterStorage = 579
        WaterPumpStationUpstreamWaterStorage = 580
        WaterPumpStationWaterPumps = 581
        WaterPumpStationConstraints = 582
        WaterPumpStationObjectives = 583
        SystemWaterPumps = 584
        WaterPumpTemplate = 585
        ListWaterPumps = 586
        WaterPumpConstraints = 587
        SystemVehicles = 588
        VehicleTemplate = 589
        ListVehicles = 590
        VehicleChargingStations = 591
        VehicleFleets = 592
        VehicleCommodities = 593
        VehicleConstraints = 594
        VehicleObjectives = 595
        SystemChargingStations = 596
        ChargingStationTemplate = 597
        ListChargingStations = 598
        ChargingStationReserves = 599
        ChargingStationNode = 600
        ChargingStationCommodities = 601
        SystemFleets = 602
        FleetTemplate = 603
        ListFleets = 604
        FleetCompanies = 605
        SystemCompanies = 606
        CompanyTemplate = 607
        ListCompanies = 608
        CompanyFuels = 609
        CompanyEmissions = 610
        CompanyReserves = 611
        CompanyRegions = 612
        CompanyVehicles = 613
        CompanyCompanies = 614
        CompanyCommodities = 615
        CompanyFacilities = 616
        CompanyMarkets = 617
        CompanyConstraints = 618
        CompanyObjectives = 619
        SystemCommodities = 620
        CommodityTemplate = 621
        ListCommodities = 622
        CommodityMarkets = 623
        CommodityConstraints = 624
        CommodityObjectives = 625
        SystemProcesses = 626
        ProcessTemplate = 627
        ListProcesses = 628
        ProcessPrimaryInput = 629
        ProcessPrimaryOutput = 630
        ProcessSecondaryInputs = 631
        ProcessSecondaryOutputs = 632
        ProcessConstraints = 633
        ProcessObjectives = 634
        SystemFacilities = 635
        FacilityTemplate = 636
        ListFacilities = 637
        FacilityPrimaryInputs = 638
        FacilityPrimaryOutputs = 639
        FacilitySecondaryInputs = 640
        FacilitySecondaryOutputs = 641
        FacilityPrimaryProcess = 642
        FacilitySecondaryProcesses = 643
        FacilityWarmUpProcess = 644
        FacilityMaintenances = 645
        FacilityFlowNodes = 646
        FacilityEntities = 647
        FacilityConstraints = 648
        FacilityObjectives = 649
        FacilityConditions = 650
        SystemMaintenances = 651
        MaintenanceTemplate = 652
        ListMaintenances = 653
        MaintenancePrerequisites = 654
        MaintenanceConstraints = 655
        MaintenanceObjectives = 656
        SystemFlowNetworks = 657
        FlowNetworkTemplate = 658
        ListFlowNetworks = 659
        FlowNetworkCommodity = 660
        FlowNetworkFacilities = 661
        FlowNetworkFlowNodes = 662
        FlowNetworkFlowStorages = 663
        FlowNetworkConstraints = 664
        FlowNetworkObjectives = 665
        SystemFlowNodes = 666
        FlowNodeTemplate = 667
        ListFlowNodes = 668
        FlowNodeEntities = 669
        FlowNodeMarkets = 670
        FlowNodeConstraints = 671
        FlowNodeObjectives = 672
        SystemFlowPaths = 673
        FlowPathTemplate = 674
        ListFlowPaths = 675
        FlowPathFlowNodeFrom = 676
        FlowPathFlowNodeTo = 677
        FlowPathEntities = 678
        FlowPathConstraints = 679
        FlowPathObjectives = 680
        SystemFlowStorages = 681
        FlowStorageTemplate = 682
        ListFlowStorages = 683
        FlowStorageFlowNode = 684
        FlowStorageEntities = 685
        FlowStorageConstraints = 686
        FlowStorageObjectives = 687
        SystemEntities = 688
        EntityTemplate = 689
        ListEntities = 690
        EntityCommodities = 691
        EntityConstraints = 692
        EntityObjectives = 693
        SystemMarkets = 694
        MarketTemplate = 695
        ListMarkets = 696
        MarketEntities = 697
        MarketConstraints = 698
        MarketObjectives = 699
        SystemConstraints = 700
        ConstraintTemplate = 701
        ListConstraints = 702
        ConstraintConditions = 703
        SystemObjectives = 704
        ObjectiveTemplate = 705
        ListObjectives = 706
        SystemDecisionVariables = 707
        DecisionVariableTemplate = 708
        ListDecisionVariables = 709
        DecisionVariableConstraints = 710
        DecisionVariableDefinition = 711
        DecisionVariableObjectives = 712
        SystemNonlinearConstraints = 713
        NonlinearConstraintTemplate = 714
        ListNonlinearConstraints = 715
        NonlinearConstraintDecisionVariableX = 716
        NonlinearConstraintDecisionVariableY = 717
        SystemDataFiles = 718
        ListDataFiles = 719
        SystemVariables = 720
        ListVariables = 721
        VariableConstraints = 722
        VariableObjectives = 723
        VariableVariables = 724
        VariableConditions = 725
        SystemTimeslices = 726
        ListTimeslices = 727
        SystemGlobals = 728
        ListGlobals = 729
        SystemScenarios = 730
        ListScenarios = 731
        SystemWeatherStations = 732
        ListWeatherStations = 733
        WeatherStationGasNode = 734
        WeatherStationGasStorages = 735
        WeatherStationGasDemands = 736
        SystemModels = 737
        ModelScenarios = 738
        ModelReliability = 739
        ModelHorizon = 740
        ModelReport = 741
        ModelPreview = 742
        ModelLTPlan = 743
        ModelPASA = 744
        ModelMTSchedule = 745
        ModelSTSchedule = 746
        ModelStochastic = 747
        ModelTransmission = 748
        ModelProduction = 749
        ModelCompetition = 750
        ModelPerformance = 751
        ModelDiagnostic = 752
        ModelInterleaved = 753
        SystemProjects = 754
        ProjectModels = 755
        ProjectHorizon = 756
        ProjectReport = 757
        SystemHorizons = 758
        SystemReports = 759
        ReportMasterFilter = 760
        ReportObjectFilter = 761
        ReportLists = 762
        SystemStochastic = 763
        SystemPreview = 764
        SystemLTPlan = 765
        LTPlanVariables = 766
        LTPlanTransmission = 767
        LTPlanProduction = 768
        LTPlanCompetition = 769
        LTPlanPerformance = 770
        LTPlanDiagnostic = 771
        SystemPASA = 772
        PASATransmission = 773
        PASAProduction = 774
        PASACompetition = 775
        PASAPerformance = 776
        PASADiagnostic = 777
        SystemMTSchedule = 778
        MTScheduleVariables = 779
        MTScheduleTransmission = 780
        MTScheduleProduction = 781
        MTScheduleCompetition = 782
        MTSchedulePerformance = 783
        MTScheduleDiagnostic = 784
        SystemSTSchedule = 785
        STScheduleTransmission = 786
        STScheduleProduction = 787
        STScheduleCompetition = 788
        STSchedulePerformance = 789
        STScheduleDiagnostic = 790
        SystemTransmission = 791
        SystemProduction = 792
        SystemCompetition = 793
        SystemPerformance = 794
        SystemDiagnostics = 795
        SystemLists = 796
        ListLists = 797
        SystemLayouts = 798
        LayoutTemplate = 799
        ListLayouts = 800
        LayoutStorages = 801
        LayoutNodes = 802
        LayoutHeatNodes = 803
        LayoutGasNodes = 804
        LayoutWaterNodes = 805
        LayoutFlowNodes = 806
    '''

    # Parse the CollectionEnum string
    collection_mapping = parse_collection_enum(collection_enum_str)

    # Create the mapping
    create_mapping(df, collection_mapping)

if __name__ == '__main__':
    main()
