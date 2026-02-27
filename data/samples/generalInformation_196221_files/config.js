var epzCommonConfig = (function () {

    var mod = {
        contextPath: undefined,
        smartSearchEnable: undefined,

        urls: {
            epzMainPublicUr: undefined,
            epzNsiUrl: undefined,
            epzOrderUrl: undefined,
            epzOrderPlanUrl: undefined,
            epzOrderClauseUrl: undefined,
            epzContractUrl: undefined,
            epzContractReportingUrl: undefined,
            epzCustomerReportsUrl: undefined,
            epzOrganizationUrl: undefined,
            epzDishonestSupplierUrl: undefined,
            epzComplaintUrl: undefined,
            epzBankGuaranteeUrl: undefined,
            epzInspectionPlanUrl: undefined,
            epzUnscheduledInspectionUrl: undefined,
            epzControlResultUrl: undefined,
            epzDizkUrl: undefined,
            epzEsUrl: undefined,
            epzFarmUrl:undefined
        }
    };

    mod.initCommonConfig = function (config) {
        mod.contextPath = config.contextPath;
        mod.smartSearchEnable = config.smartSearchEnable;

        mod.urls.epzMainPublicUrl= config.urls.epzMainPublicUrl;
        mod.urls.epzNsiUrl= config.urls.epzNsiUrl;
        mod.urls.epzOrderUrl= config.urls.epzOrderUrl;
        mod.urls.epzOrderPlanUrl= config.urls.epzOrderPlanUrl;
        mod.urls.epzOrderClauseUrl= config.urls.epzOrderClauseUrl;
        mod.urls.epzContractUrl= config.urls.epzContractUrl;
        mod.urls.epzContractReportingUrl= config.urls.epzContractReportingUrl;
        mod.urls.epzCustomerReportsUrl= config.urls.epzCustomerReportsUrl;
        mod.urls.epzOrganizationUrl= config.urls.epzOrganizationUrl;
        mod.urls.epzDishonestSupplierUrl= config.urls.epzDishonestSupplierUrl;
        mod.urls.epzComplaintUrl= config.urls.epzComplaintUrl;
        mod.urls.epzBankGuaranteeUrl= config.urls.epzBankGuaranteeUrl;
        mod.urls.epzInspectionPlanUrl= config.urls.epzInspectionPlanUrl;
        mod.urls.epzUnscheduledInspectionUrl= config.urls.epzUnscheduledInspectionUrl;
        mod.urls.epzControlResultUrl= config.urls.epzControlResultUrl;
        mod.urls.epzDizkUrl= config.urls.epzDizkUrl;
        mod.urls.epzEsUrl = config.urls.epzEsUrl;
        mod.urls.epzFarmUrl = config.urls.epzFarmUrl;
    };

    return mod;
}());
