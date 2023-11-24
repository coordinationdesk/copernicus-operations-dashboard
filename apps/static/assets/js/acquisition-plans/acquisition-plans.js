/*
Copernicus Operations Dashboard

Copyright (C) ${startYear}-${currentYear} ${Telespazio}
All rights reserved.

This document discloses subject matter in which TPZ has
proprietary rights. Recipient of the document shall not duplicate, use or
disclose in whole or in part, information contained herein except for or on
behalf of TPZ to fulfill the purpose for which the document was
delivered to him.
*/
// List of missions for which KML of acquistion plans is generated
// acquisitionKmlMissions = ['S1', 'S2']; NO Orbit KML
acquisitionKmlMissions = ['S1', 'S2', 'S3', 'S5']; // S3/S5 KML frorbit

missionKmlDatatakeId = {
    'S1': "DatatakeId",
    'S2': "ID",
    'S3': "DatatakeId",
    'S5': "DatatakeId"
}

missionDatatakeId = {
    'S1': "DatatakeId",
    'S2': "ID",
    'S3': "datatake_id",
    'S5': "datatake_id"
}

satelliteNoradId = {'S1A': 39634, 'S1B': 41456,
                    'S2A': 40697, 'S2B': 42063,
                    'S3A': 41335, 'S3B': 43437,
                    'S5P': 42969};

useDatePicker = false;

class MissionAcquisitionDates extends EventTarget {

    /**
     * Class responsible for managing selection of acquisition Plan days
     * It retrieves a table containing the dates of available acquisition plans
     * for each mission/satellite.
     * Accessing the retrieved values, Select controls can updated their own value lists.
     * An event is generated whenever a Day is selected, or a different satellite
     * is chosen by the user.
     * 
     * @type {MissionAcquisitionDates}
     */
    constructor() {

        super();

        this.acqplansDates = {
                    'S1A': {label: 'Sentinel-1A', mission: 'S1', dates: null},
                    'S1B': {label: 'Sentinel-1B', mission: 'S1', dates: null},
                    'S2A': {label: 'Sentinel-2A', mission: 'S2', dates: null},
                    'S2B': {label: 'Sentinel-2B', mission: 'S2', dates: null},
                    'S3A': {label: 'Sentinel-3A', mission: 'S3', dates: null},
                    'S3B': {label: 'Sentinel-3B', mission: 'S3', dates: null},
                    'S5P': {label: 'Sentinel-5P', mission: 'S5', dates: null},
        };

        this.selectedParams = {
            'mission': null,
            'satellite': null,
            'day': null
        };

        this._daySelectionId = 'acquisition-plans-day-select'; // 'datepicker'; // 'acquisition-plans-day-select';

        if (useDatePicker) {
            this._daySelectionId = 'datepicker';
        }
    }

    /**
     * Initializing function
     * @returns {undefined}
     */
    init() {

        // Init event listeners
        // TODO: Use a different Event Listener for Datepicker
        var satellite_sel = document.getElementById('acquisition-plans-satellite-select');
        satellite_sel.addEventListener('change', this.onSatelliteSelectChange.bind(this));
        var date_sel = document.getElementById('acquisition-plans-day-select');
        date_sel.addEventListener('change', this.onPlanDaySelectChange.bind(this));

        if (useDatePicker) {
            $('#datepicker').datepicker({
               format: 'dd/mm/yyyy',
               //startDate: '-15d',
               todayHighlight: true,
               todayBtn: true,
               autoclose: true,
               beforeShowDay: this.isDateInKml.bind(this)
            });
            $('#datepicker').show();
            $('#acquisition-plans-day-select').remove(); // hide();
        } else {
            $('#datepicker').remove(); // hide
            $('#acquisition-plans-day-select').show();
        }

        // Set Empty Arrays for dates!
        for (const satellite of Object.keys(this.acqplansDates)) {
            this.acqplansDates[satellite].dates = new Array();
        }

        // Create Structure to hold Acquisition Plans Availability
        // Register event handler for Select controls change events
        this.loadAcquisitionPlansAvailability();
    }

    isDateInKml(selDate) {

        // Convert date to format stored in internal table
        var day_item = moment(selDate).format('yyyy-MM-DD');
        // Return true if converted date is present in current day set
        var sel_sat = this.selectedParams.satellite;
        var dayList = this.acqplansDates[sel_sat].dates;
        // Return dayList.indexOf(dt_ddmmyyyy) != -1
        return true;
    }
    
    loadAcquisitionPlansAvailability() {

        // Acknowledge the invocation of rest APIs
        console.info("Starting retrieval of Acquisition Plan Availablity...");

        // Add class Busy to charts
        var that = this;
        var acq_plans_api_name = 'acquisitions/acquisition-plan-days';
        var ajaxPromises = asyncAjaxCall('/api/' + acq_plans_api_name, 'GET', {},
            that.successAcquisitionPlansAvailability.bind(that), that.failureAcquisitionPlansAvailability);

        // Execute asynchronous AJAX call
        ajaxPromises.then(function() {
            console.log("Received all results!");
            var dialog = document.getElementById('window');
            if (dialog !== null) {
                // TODO: REMOVE SPINNER
                dialog.show();
                document.getElementById('exit').onclick = function() {
                    console.log("Click");
                    dialog.close();
                };
            }
        });
    }

    successAcquisitionPlansAvailability(response) {
        var json_resp = response;
        console.debug("Acquisition Plans Availability - Received response:", json_resp);
        // load response
        // set initial values for Select Controls
        for (const  [mission, missionData] of  Object.entries(json_resp)) {
            console.log("From response, extracting mission ", mission);
            for (const [satellite, dayList] of Object.entries(missionData)) {
                console.log("From response, extracting satellite ", satellite, ", days: ", dayList);
                // Check if Mission/satellite are present in Availabilities table
                this.acqplansDates[satellite].dates.push.apply(this.acqplansDates[satellite].dates, dayList);
            }
        }
        
        // Fill Satellite Select
        var response_satellites = [].concat.apply([],
            Object.values(json_resp).map(function(mission_data) {return Object.keys(mission_data);}));
        console.log("Satellites in response: ", response_satellites);
        var satellite_sel = document.getElementById('acquisition-plans-satellite-select');
        var that = this;
        var satellite_labels = response_satellites.map(function (satellite) { 
            return [satellite, that.acqplansDates[satellite].label];
        });
        this._fill_select_element(satellite_sel, satellite_labels);
        this._select_element(satellite_sel, satellite_labels[0][0]);
    }

    _select_element(sel_elem, sel_option) {
                    // Select First item
        sel_elem.value = sel_option;
        //sel_elem.selectedIndex = 0;
        // Activate Event Handlers
        sel_elem.dispatchEvent(new Event('change'));
    }

    _fill_select_element(sel_elem, value_labels) {

        // console.log("Filling element ", sel_elem, " with value labels: ", value_labels);
        // Clear Select Element
        sel_elem.options.length = 0;
        // Load value/Labels on Select Element
        value_labels.forEach(function([value, label]) {
            sel_elem.add(new Option(label, value));
        });
    }

    failureAcquisitionPlansAvailability(response){
        console.error(response);
        return;
    } 

    onSatelliteSelectChange(ev) {
        var satellite = ev.target.value;
        console.info("Acquisition Selection: satellite changed to " + satellite);
        this.selectedParams.satellite = satellite;
        this.selectedParams.mission = this.acqplansDates[satellite].mission;

        // Fill Date Select with label/values from configuration for this satellite value
        var day_list = this.acqplansDates[satellite].dates;
        var day_select_items = day_list.map(function(day_str) {
            var item_date = moment(day_str, 'yyyy-MM-DD').format("DD MMM yyyy");
            return [day_str, item_date];
        });

        // day_list.map(day_str => [day_str, moment(day_str, 'yyyy-MM-DD').toLocaleString()])
        // Build list of date formatted, day list
        console.log("List of dates for selected satellite: ", day_list);
        this.setAvailableDays(day_select_items);
    }
    
    /**
     * Fills the control to select Acquisition Day
     * with a list of available Day dates
     * 
     * @param {type} days_items a list of pairs: the string to be 
     *    inserted in the retrieval API request, and the string to 
     *    be presented to user
     * @returns {undefined}
     */
    setAvailableDays(days_items) {
        var date_sel = document.getElementById(this._daySelectionId);
        if (!useDatePicker) {
            this._fill_select_element(date_sel, days_items);
            this.selectDefaultDay(date_sel);
        }
        // Save days so that DatePicker makes only them enabled
        // this._save_datepicker_available_days(days_items);
    }

    datepickerSelectDefaultDay(date_sel) {
        return;
    }

    selectDefaultDay(date_sel) {

        // Check whether the dates list contains the entry for the
        // current date; if so, select it. Otherwise, select the last date
        var today = new Date();
        // Format using the value format
        var today_item = moment(today).format('YYYY-MM-DD');
        var exists = $('#'+this._daySelectionId + ' > option').filter(function(){ return $(this).val() == today_item; }).length;
        if (exists) {
            this._select_element(date_sel, today_item);
        } else {

            // Select last element in select
            var lastValue = date_sel.options[date_sel.options.length - 1].value;
            this._select_element(date_sel, lastValue);
        }

    }

    getSelection() {
        return self.selectedParams;
    }

    onPlanDaySelectChange(ev) {
        console.log("Selected Date: ", ev);
        var selectedDay = ev.target.value;
        this.selectedParams.day = selectedDay;
        console.log("Current selection: ", this.selectedParams);

        // Dispatch event for the Acquisition Plan Viewer class
        var planDayEvent = new CustomEvent('changeDate',{
                detail: this.selectedParams,
                cancelable: true });
        this.dispatchEvent(planDayEvent);
    }
}

class AcquisitionPlansViewer {

    /**
     * Show on a Globe map the acquisition plans for satellite 
     * datatakes for one or more days (TBC)
     * Show the selected satellite(s) orbit and an icon representing
     * the satellite moving along the orbit
     * @type type
     */

    /**
     * AcquisitionPlans
     * @returns {ProductTimeliness}
     */
    constructor() {
        this.parametersSelection = new MissionAcquisitionDates();
        this.currentKMLDatasources = Array();
        this.datatakes_list = Array();
    }

    init() {

        // Hide the drop-down menu to select the time range
        $('#time-period-select-container').hide();

        // Hide footer
        $('.footer').hide();

        // Instantiate and activate PlansCoverage Class
        this.currentMission = null;
        this.parametersSelection.init();

        // MAKE parameters selection init a Promise, and load CESIUM after promise resolution
        // Register the event callbacks for Plans Coverage Select activation (the day activation)
        // Register event callback for Time period select
        this.parametersSelection.addEventListener('changeSatellite', this.on_satellite_change.bind(this));
        this.parametersSelection.addEventListener('changeDate', this.on_plan_date_change.bind(this));

        // Initialize Cesium properties
        // If true, move Animation Time to restart after currently tracked entity time.
        this._moveAnimationToTrackedEntity = false;
        this._lastAnimationTime = null;

        // Set access token to null to avoid warnings
        // Cesium.Ion.defaultAccessToken = null;
        Cesium.Ion.defaultAccessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIyMTM2NzEzZC0wOGYzLTRhNmItYmJmYi1jN2YwNjgyMzlmMjUiLCJpZCI6MTA2NTMzLCJpYXQiOjE2OTI5NzA0NTl9.ebZYtBUTUdKJMbinwljI8z-2x1TsNX3k8HSnL_ufGIc';

        // Load the Cesium JS plugin
        this.viewer_widget = new Cesium.Viewer("container-acq-plans", {
            timeline: true,
            animation: true,

            // Prevent data sources from overriding clock settings
            automaticallyTrackDataSourceClocks : false,

            // Disable baseLayer Picker and geocoder, as they use external services
            baseLayerPicker: false,
            geocoder: false,
            // skyBox: false,

            // Use Copernicus Sentinel-2 imagery
            imageryProvider: new Cesium.IonImageryProvider({ assetId: 3954 })

            // Keep as backup the internal loew-res free map
            // baseLayer: Cesium.ImageryLayer.fromProviderAsync(
            // Cesium.TileMapServiceImageryProvider.fromUrl(
            //     Cesium.buildModuleUrl("Assets/Textures/NaturalEarthII")
            // ))
        });

        // Enable atmosphere and lighting
        this.viewer_widget.scene.globe.showSkyAtmosphere = true;
        this.viewer_widget.scene.globe.showGroundAtmosphere = true;
        this.viewer_widget.scene.globe.enableLighting = true;

        // Set the Animation View Widget properties
        // var multipliers = new Array(1, 120, 180, 300, 600, 1200, 1800, 3600);
        // Cesium.AnimationViewModel.defaultTicks = multipliers;
        // this.viewer_widget.animation.viewModel.snapToTicks = true;
        // this.viewer_widget.animation.viewModel.setShuttleRingTicks(multipliers);
        // this.viewer_widget.clock.multiplier = 180; // Step are 10 minutes long
        // this.viewer_widget.clock.clockStep = Cesium.ClockStep.SYSTEM_CLOCK_MULTIPLIER;
        // this.viewer_widget.clock.clockRange = Cesium.ClockRange.CLAMPED;

        // Adapt the Cesium viewer to the current screen
        this.resizeWindow();

        // Display satellite orbits
        asyncAjaxCall('/api/acquisitions/satellite/orbits', 'GET', {},
            this.successLoadSatellitesOrbits.bind(this), this.failureLoadSatellitesOrbits);

        // Display acquisition stations
        asyncAjaxCall('/api/acquisitions/stations', 'GET', {},
            this.successLoadAcquisitionStations.bind(this), this.failureLoadAcquisitionStations);
    }

    // SatelliteChange: the event handler is attached to MissionAcquisitionDays object
    // event selection_change and received the couple satellite/day
    on_satellite_change(selectionEv) {
        this.reset_camera_view();
    }
    reset_camera_view() {
        // Force reset of camera position
        //this.viewer_widget.homeButton.viewModel.command();
        this.viewer_widget.camera.flyHome(0);
    }

    // DayChange: the event handler is attached to MissionAcquisitionDays object
    // event selection_change and received the couple satellite/day
    on_plan_date_change(selectionEv) {
        this.currentMission = selectionEv.detail.mission;
        var satelliteSel = selectionEv.detail.satellite;
        var daySel = selectionEv.detail.day;
        // this.reset_camera_view();
        console.debug("Activated EventHanlder; mission: ", this.currentMission,
                ",satellite: ", satelliteSel, ", date: ", daySel);
        // this.updateDateInterval(time_period_sel.value);
        if (acquisitionKmlMissions.includes(this.currentMission) ) {
            this.loadAcquisitionPlan(this.currentMission, satelliteSel, daySel);
        } else {
            // Use a different function to retrieve acquisition Plans
            // that are not generated in KML format
            this.loadAcquisitionDatatakes(this.currentMission, satelliteSel, daySel);
        }
        this.reset_camera_view();
    }

    successLoadSatellitesOrbits(response) {
        this.viewer_widget.dataSources.add(
            Cesium.CzmlDataSource.load(response));
    /*
        var that = this;
        var orbitDsPromise = Cesium.CzmlDataSource.load(response);
        orbitDsPromise.then(function (orbitDS) {
            that.viewer_widget.dataSources.add(orbitDS);
            console.log("Completed loading CZML Orbits on Viewr");
            orbitDS.name = "SatelliteOrbits";
                });
        */
    }

    failureLoadSatellitesOrbits(response) {
        console.error("Unable to load satellite orbit");
        console.error(response);
    }

    successLoadAcquisitionStations(response) {
        this.viewer_widget.dataSources.add(
            Cesium.CzmlDataSource.load(response)
        );
    }

    failureLoadAcquisitionStations(response) {
        console.error("Unable to load acquisition stations");
        console.error(response);
    }

    loadAcquisitionDatatakes(mission, satellite, plan_day) {
        this.clearAcquisitionPlans();
        this.showSpinner();
        var acq_plans_api_name = 'acquisitions/acquisition-datatakes';
        console.log("Loading Acquisition Plans for satellite " + satellite + ", day " + plan_day);
        var urlParamString = `${mission}/${satellite}/${plan_day}`;
        console.log("Parameters for API URL: "+urlParamString);
        var that = this;
        // accept = 'application/xml'
        var ajaxPromises = asyncAjaxCall('/api/' + acq_plans_api_name + '/'+urlParamString, 'GET', {},
            that.successLoadAcquisitionDatatakes.bind(that), that.errorLoadAcquisitionPlan);

        // Execute asynchronous AJAX call
        ajaxPromises.then(function() {
             console.log("Received all results!");
             that.removeSpinner();
            }
        );
    }

    loadAcquisitionPlan(mission, satellite, plan_day) {
        this.clearAcquisitionPlans();
        this.showSpinner();
        var acq_plans_api_name = 'acquisitions/acquisition-plans';
        console.log("Loading Acquisition Plans for satellite " + satellite + ", day " + plan_day);

        // TODO: Clear previous KML if already loaded
        // TODO; put Waiting Spinner

        // Acknowledge the invocation of rest APIs
        console.info("Starting retrieval of Acquisition Plan KML...");

        // Add class Busy to charts
        // var mission = 'S1';
        var urlParamString = `${mission}/${satellite}/${plan_day}`;
        console.log("Parameters for API URL: "+urlParamString);
        var that = this;
        // accept = 'application/xml'
        var ajaxPromises = asyncAjaxDownloadXml('/api/' + acq_plans_api_name + '/'+urlParamString, 'GET', {},
            that.successLoadAcquisitionPlan.bind(that), that.errorLoadAcquisitionPlan);

        // Execute asynchronous AJAX call
        ajaxPromises.then(function() {
             console.log("Received all results!");
             that.removeSpinner();
            }
        );
    }

    showSpinner() {
        console.log("Show Spinner");
        $('.card', document.getElementById('acquisition-plans-container')).eq(0).html(
                '<div class="spinner">' +
                    '<div class="bounce1"></div>' +
                    '<div class="bounce2"></div>' +
                    '<div class="bounce3"></div>' +
                '</div>');
       $('.spinner-border')[0].hidden = false;

       //$('#acquisition-plans-spinner').hidden = false;
       $('#acquisition-plans-spinner').show();

       // var a = document.getElementsByClassName('spinner-border');
       // a[0].style.display='block' ;
       // show spinner, disable page
    }

    removeSpinner(){
        console.log("Hide Spinner");
        //$('#acquisition-plans-spinner').hidden = true;
        $('#acquisition-plans-spinner').hide();
        //back to normal!
    }

    successLoadAcquisitionDatatakes(response) {
        console.log("Acquisition Datatakes Response: ",response);
        var datatakes = format_response(response);
        var dtList = this.extractAcquisitionDatatakeIdList(datatakes);
        this.writeDatatakesList(dtList); // (dtListAttrs)
    }

    successLoadAcquisitionPlan(response){

        // TODO: Modify response, to include parameters: Mission, satellite, day
        // Acknowledge the successful retrieval of downlink operations
        // var kml_result = format_response(response);
        var kml_result = response;
        // console.log("received response:", json_resp);
        console.info('Acquisition Plan successfully retrieved');

        // Parse response
        // mission (just for check)
        // Update the KML on World view
        this.drawAcquisitionPlan(kml_result); 
    }

    errorLoadAcquisitionPlan(response){
        console.error(response);
        var from = 'top';
        var align = 'center';
        var state = 'error';

        // Read from response: error, errorText
        var content = {
                title: 'Request Error',
                message: 'Error '+ response.status +
                    '<p>' + response.statusText + '</p>',
                icon: 'flaticon-round'
        };

        // Display notification message
        msgNotification(from, align, state, content);
    }

    /**
     *
     * @param {string} pieId | the id of the lement containing the canvas div
     * @param {number} timeThreshold | the number of hours reprsenting the time Threshold for
     *  the data represented
     * @param {Object} data | a two element object containing:
     *      on_time: the number of products published according to the timliness
     *      constraint;
     *      total_count: the total number of generated products (published or not)
     * @returns N/A
     */
    drawAcquisitionPlan(kmlString) {

        // Acknowledge drawing of KML
        console.log("Drawing KML for Acq Plan ");

        var options = {
            // camera : this.viewer_widget.scene.camera,
            // canvas : this.viewer_widget.scene.canvas,
            // clampToGround: true,
            credit: "ESA"
        };
        // Remove first previous loaded KML DSulian

        var newAcqPlanDSpromise = Cesium.KmlDataSource.load(kmlString, options);
        var that = this;
        newAcqPlanDSpromise.then(function (kmlDS) {

            // that.viewer_widget.flyTo(newAcqPlanDSpromise);
            console.log("Setting KML Datasource clock properties");
            kmlDS.clock.multiplier = 300; // Step are 10 minutes long
            kmlDS.clock.clockStep = Cesium.ClockStep.SYSTEM_CLOCK_MULTIPLIER;
            kmlDS.clock.clockRange = Cesium.ClockRange.CLAMPED;
            kmlDS.entities.values.forEach(function (kmlEntity) {
                kmlEntity.show = true;
            });
            kmlDS.entities.show = true;
            if (that.currentKMLDatasources.length > 0 ) {
                that.viewer_widget.dataSources.remove(that.currentKMLDatasources[0], true);
                that.currentKMLDatasources.pop();
            }
            that.currentKMLDatasources.push(kmlDS);
            //that.viewer_widget.clock.canAnimate = true;
            that.viewer_widget.clock.shouldAnimate = true;
            console.log("Adding KML Source to Viewer");
            that.viewer_widget.dataSources.add(kmlDS).then(
                function(data) {
                    //data.clock.shouldAnimate = true;
                    console.log("Completed loading KML Source on Viewr");
                    // var dtListAttrs =
                    var dtList = that.extractDatatakeIdList(data);
                    that.writeDatatakesList(dtList); // (dtListAttrs)
                });
        });
    }

    extractDatatakeIdList(kmlSource) {

        // Acknowledge the creation of the datatakes list
        console.debug("Extracting Datatakes from Datasource");

        // Note: we need a pointer to the entity in addition to the name
        // saves the list of DataTake ID, associated with the parent Entity name
        var dtIdProperty = missionKmlDatatakeId[this.currentMission];
        var that = this;
        this.datatakes_list = new Map();

        // Map on Entities on KML Source (i.e. Placemark elements,
        // loaded with metadata (extendedData) )
        var dtIdValues = kmlSource.entities.values.map(function(val) {
            if (val.kml.extendedData) {
                var dtId = val.kml.extendedData[dtIdProperty].value;
                var dtLabel = dtId;
                if (that.currentMission === 'S2') {

                    // Extract Mode and append to label
                    dtLabel = dtLabel + " ("+ val.kml.extendedData['Mode'].value+")";
                }

                // Datatakes: map Datatake ID to Datatake Entity
                // It was Label to [ ID, Entity ]
                that.datatakes_list.set(dtId, val);
                var dt_publicationStatus = val.kml.extendedData['Publication Status'];
                if (dt_publicationStatus) {
                    dt_publicationStatus = dt_publicationStatus['value'];
                }
                var dt_status = that.decodeDatatakeCompletenessStatus(dt_publicationStatus);
                // TODO: Change: insert val (whole entity) instead of dtId
                // Do not create and fill datatakes_list
                return [dtLabel, dtId, dt_status];
            }
        });

       // this.datatakes_list = new Map(dtIdValues.filter(n=> n));
       // Return list without None elements!
       return dtIdValues.filter(n=> n);
    }

    extractAcquisitionDatatakeIdList(acq_datatakes) {
        console.debug("Extracting Datatakes from Acquisition Datatakes");
        var dtIdProperty = missionDatatakeId[this.currentMission];
        var that = this;
        this.datatakes_list = new Map();
        var dtIdValues = acq_datatakes.map(function(acq_dt) {
                var dtId = acq_dt[dtIdProperty];
                var dtLabel = dtId;
                that.datatakes_list.set(dtId, acq_dt);
                // TODO: Decode only PUB STATUS
                var pubDatatakeStatus = acq_dt.completeness_status.PUB.status;
                console.debug("Acquisition Datatake pub status: ", pubDatatakeStatus);
                var dt_status = that.decodeDatatakeCompletenessStatus(pubDatatakeStatus);
                console.debug("Decoded status: ", dt_status);
                return [dtLabel, dtId, dt_status];
        });
       // Return list without None elements!
       return dtIdValues.filter(n=> n);
    }

    writeDatatakesList(datatakesList) {

        // Acknowledge the creation of the panel with the datatakes list
        console.debug("Building dropdown menu with the Datatakes List");

        // Write on the proper panel a list of links, with label the Datatake Id
        // and associated a reference (or the name) of the datatake Entity
        // An event is registered on the list, that flies to the selected entity
        console.debug("Datatakes table on class: ", this.datatakes_list);

        // Loop over each data take and append the corresponding entry
        datatakesList.forEach(function(value) {
            // TODO: CHange: pass directly the DT Entity as value to the option
            var [dt_label, dt_id, dt_status] = value;
            var escape_circle_status = '&#9899';
            if (dt_status === 'ok') escape_circle_status = '&#128994';
            if (dt_status === 'partial') escape_circle_status = '&#128992';
            if (dt_status === 'failed') escape_circle_status = '&#128308';
            $('#acq-datatakes-select').append(
                '<option value="' + dt_id + '">' + dt_label + '&nbsp;&nbsp;' +
                    escape_circle_status +
                '</option>'
            );
        });

        // Manage selection changes
        var that = this;
        $('#acq-datatakes-select').change(function() {
            // Read current selected value of select element
            var dt = $(this).val();
            that.stopKmlAnimation();
            that.flyToDatatake(dt);
        });

        // Acknowledge the completion of the drop-down datatake list
        console.debug("Completed building dropdown menu with Datatakes List");
    }

    // Or just perform on this.viewer_widget.camera flyToHome!
    // call before performing a new flyTo, if a previous flyto was performed
    setHeightKm(heightInKilometers) {
        var cartographic = new Cesium.Cartographic();
        var cartesian = new Cesium.Cartesian3();
        var camera = this.viewer_widget.scene.camera;
        var ellipsoid = this.viewer_widget.scene.mapProjection.ellipsoid;
        ellipsoid.cartesianToCartographic(camera.position, cartographic);
        cartographic.height = heightInKilometers * 1000;  // convert to meters
        ellipsoid.cartographicToCartesian(cartographic, cartesian);
        camera.position = cartesian;
    }

    saveAnimationStatus() {
        if (!this._moveAnimationToTrackedEntity) {
            // Save clock status
            this._lastAnimationTime = this.viewer_widget.clock.currentTime;
        }
    }

    stopKmlAnimation() {
        this.viewer_widget.clockViewModel.shouldAnimate = false;
    }

    flyToDatatake(datatake_id) {
        var dt_entity = this.datatakes_list.get(datatake_id);
        var dt_name = dt_entity.name; // this.datatakes_list.get(datatake_id); // dt_entity.name;
        if (acquisitionKmlMissions.some(word => datatake_id.startsWith(word))) {
        //if (datatake_id.startsWith('S1') || datatake_id.startsWith('S2') || datatake_id.startsWith('S3')) {

            // TODO : handle non existing entity
            // Find entity in KML Source Collection with specified name
            console.log("Entity with id ", datatake_id,", name ", dt_name, ":", dt_entity);
            this.selectViewerTarget(dt_entity);
            this.viewer_widget.clock.currentTime = dt_entity.availability.start;
            var flyPromise = this.viewer_widget.flyTo(dt_entity);
            var that = this;
            flyPromise.then(function (result) {
                if (result) {
                    console.log("Displaying details");
                    that.viewer_widget.scene.requestRender();
                }
                else  {
                    console.log('Flyto was canceled or entity not in scene: ', result);
                }
            }).catch(function(error) {
                console.log(error);
            });

        } else {

            // TEMPORARY: For S3/S5: set time,
            // Satellite is defined by first 3 chars in datatake id
            var dt_satellite = datatake_id.substring(0,2);
            this.flyToSatellite(dt_satellite, dt_entity.observation_time_start)
        }
    }

    flyToSatellite(satellite_id, satellite_timestamp) {
            // TODO: Find another way to select Orbit Datasource
            var czmlDs =  this.viewer_widget.dataSources.get(1);
            var satNoradId = satelliteNoradId[satellite_id];
            var dt_sat_entity = czmlDs.entities.getById(satNoradId);

            // Find Satellite for this Datatake
            // select CZML orbit for S3/S5, rotate to center the satellite,
            // Keep view Height
            // 2023-10-10T00:21:00.331Z
            var dt_start_julian = Cesium.JulianDate.fromIso8601(satellite_timestamp);

            // Convert time string to current Time object
            this.viewer_widget.clock.currentTime = dt_start_julian;
            this.selectViewerTarget(dt_sat_entity);
    }

    selectViewerTarget(entity) {
        this.viewer_widget.trackedEntity = entity;
        this.viewer_widget.selectedEntity = entity;
    }

    // Remove from viewer all currently loaded Datasources
    clearAcquisitionPlans() {
        this.currentKMLDatasources.forEach(function(item) {
            acquisitionPlanViewer.viewer_widget.dataSources.remove(item);
        });
        this.currentKMLDatasources = [];
        this.datatakes_list = new Map();
        $('#acq-datatakes-select').find('option').remove().end();
    }

    decodeDatatakeCompletenessStatus(statusValue) {

        // Return the datatake completeness status, on the basis of the current completeness
        // Allowed values are "ok", "partial", "failed" and "undef".
        // The completeness status will define the displayed color beside the datatake id
        if (!statusValue) {
            return "undef";
        }

        // "Planned" and "Processing" status are mapped with the same colour
        if (statusValue.includes("PLANNED")) {
            return "undef";
        } else if (statusValue.includes("PROCESSING")) {
            return "undef";
        } else if (statusValue.includes("PUBLISHED")) {
            return "ok";
        } else if (statusValue.includes("PARTIAL")) {
            return "partial";
        } else if (statusValue.includes("DELAYED")) {
            return "partial";
        } else if (statusValue.includes("LOST")) {
            return "failed";
        } else {
            return "undef";
        }
    }

    showDatatakeDetails() {

        // Retrieve the selected datatake ID from the dropdown menu
        var dt_id = $('#acq-datatakes-select').val();
        console.debug("Showing details for selected datatake: ", dt_id);

        // Invoke the retrieval of the datatake details
        // Add spinner during query
        $('#globe-datatake-details').empty();
        $('#globe-datatake-details').html(
            '<div class="spinner">' +
                '<div class="bounce1"></div>' +
                '<div class="bounce2"></div>' +
                '<div class="bounce3"></div>' +
             '</div>');

        // Acknowledge the visualization of the online help
        console.info('Showing details of datatake: ' + dt_id);

        // Retrieve the datatake details
        asyncAjaxCall('/api/worker/cds-datatake/' + dt_id, 'GET', {},
            this.successShowDatatakeDetails, this.errorShowDatatakeDetails);
    }

    successShowDatatakeDetails(response) {
        var datatake = format_response(response)[0];
        $('#globe-datatake-details').empty();
        $('#globe-datatake-details').append('<div class="form-group">' +
            '<label>Datatake ID: ' + datatake['key'] + '</label>' +
            '<label>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</label>' +
            '<label>Timeliness: ' + datatake['timeliness'] + '</label>' +
        '</div>');
        $('#globe-datatake-details').append('<div class="card">' +
            '<div class="card-body">' +
                '<div class="table-responsive"><div class="table-responsive">' +
                    '<table id="globe-basic-datatables-product-level-completeness" class="display table table-striped table-hover">' +
                        '<thead>' +
                            '<tr>' +
                                '<th>Product type</th>' +
                                '<th style="text-align: center">Status [%]</th>' +
                            '</tr>' +
                            '<tbody></tbody>' +
                        '</thead>' +
                    '</table>' +
                '</div>' +
            '</div>' +
        '</div>');
        var dataTakeDetailsTable = $('#globe-basic-datatables-product-level-completeness').DataTable({
            "sDom": "frtp",
            "createdRow": function(row, data, dataIndex) {
                $(row).find('td').eq(0).height(25);
                $(row).find('td').eq(1).height(25);
                $(row).find('td').eq(1).css('text-align', 'center');
                $(row).find('td').eq(1).css('color', 'white');
                var color = '#0aa41b';
                if (data[1] < 90 && data[1] >= 5) color = '#8c90a0';
                if (data[1] < 5) color = '#8c90a0';
                $(row).find('td').eq(1).css('background-color', color);
            }
        });
        for (var key of Object.keys(datatake)) {
            if (key.includes('local_percentage')) {
                dataTakeDetailsTable.row.add([key.replace('_local_percentage',''), datatake[key].toFixed(2)]).draw();
            }
        }
    }

    errorShowDatatakeDetails(response) {
        $('#datatake-details').append(
            '<div class="form-group">' +
                '<label>An error occurred, while retrieving the datatake details</label>' +
            '</div>');
    }

    showAcquisitionPlansOnlineHelp() {

        // Acknowledge the visualization of the online help
        console.info('Showing Acquisition Plans online help message...');

        // Auxiliary variable declaration
        var from = 'top';
        var align = 'center';
        var state = 'info';
        var content = {};
        content.title = 'Acquisition Plans';
        content.message = 'This view allows to show acquisition plans on a world projection, per each Copernicus Sentinel Mission ' +
            'and Satellite. Acquisition plans are displayed for one day, starting from 15 days in the past up to N days in the future, according to current availability.'
        content.icon = 'flaticon-round';

        // Display notification message
        msgNotification(from, align, state, content);

    }

    resizeWindow() {
        var css = document.getElementById('container-acq-plans').style;
        var height = window.screen.availHeight - 375
        css.height = height.toString() + 'px';
    }
}

let acquisitionPlanViewer = new AcquisitionPlansViewer();