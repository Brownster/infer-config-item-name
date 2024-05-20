# Refined mapping logic based on hostname and exporters, including WFO and ESXi logic
def infer_configuration_item_name(row):
    hostname = str(row['Hostnames']).lower()
    exporter_os = str(row['Exporter_name_os']).lower()
    exporter_app = str(row['Exporter_name_app']).lower()
    
    # Check for ESXi server
    if 'esx' in hostname or 'esxi' in hostname:
        return 'ESXi'
    
    # Detailed mapping logic for WFO types
    if 'exporter_verint' in exporter_os or 'exporter_verint' in exporter_app:
        if 'rec' in hostname:
            return 'WFO Rec'
        elif 'app' in hostname:
            return 'WFO App'
        elif 'db' in hostname:
            return 'WFO DB'
        elif 'fs' in hostname:
            return 'WFO FS'
        elif 'sp' in hostname:
            return 'WFO SP'
        elif 'tran' in hostname:
            return 'WFO Tran'
        else:
            return 'WFO - ?'

    # Other mappings
    if 'sbc' in hostname or 'avayasbc' in exporter_app:
        return 'SBC'
    elif 'gateway' in exporter_app:
        return 'Media Gateway'
    elif 'ems' in hostname:
        return 'EPMS'
    elif 'linux' in exporter_os:
        return 'LINUX'
    elif 'windows' in exporter_os:
        return 'WINDOWS'
    elif 'aep' in hostname:
        return 'AAEP'
    elif 'ipo' in hostname:
        return 'IPO'
    elif 'cms' in hostname:
        return 'CMS'
    elif 'nuance' in hostname:
        return 'Nuance'
    elif 'router' in hostname:
        return 'Router'
    elif 'proxy' in hostname:
        return 'Reverse proxy'
    elif 'verint' in hostname:
        return 'Verint Mobile Gateway'
    elif 'audiocodes' in hostname:
        return 'Audiocodes'
    elif 'avp' in hostname:
        return 'AVP'
    elif 'admin_jumpoff' in hostname:
        return 'Admin_jumpoff'
    elif 'aacc' in hostname:
        return 'AACC'
    elif 'aads' in hostname:
        return 'AADS'
    elif 'aam' in hostname:
        return 'AAM'
    elif 'accm' in hostname:
        return 'ACCM'
    elif 'accs' in hostname:
        return 'ACCS'
    elif 'acm' in hostname and 'vip' in hostname:
        return 'ACM -VIP'
    elif 'acm' in hostname and 'ess' in hostname:
        return 'ACM_ESS'
    elif 'acm' in hostname and 'lsp' in hostname:
        return 'ACM_LSP'
    elif 'acra' in hostname:
        return 'ACRA'
    elif 'ads/sal' in hostname:
        return 'ADS/SAL'
    elif 'aes' in hostname:
        return 'AES'
    elif 'aic' in hostname:
        return 'AIC'
    elif 'ams' in hostname:
        return 'AMS'
    elif 'avp utility' in hostname:
        return 'AVP Utility'
    elif 'azure proxy' in hostname:
        return 'Azure proxy'
    elif 'baas' in hostname:
        return 'BaaS'
    elif 'breeze' in hostname:
        return 'Breeze'
    elif 'callback' in hostname:
        return 'Callback'
    elif 'crs' in hostname:
        return 'CRS'
    elif 'cxp' in hostname:
        return 'CXP'
    elif 'dc' in hostname:
        return 'DC'
    elif 'esxi' in hostname:
        return 'ESXi'
    elif 'idrac' in hostname:
        return 'iDrac'
    elif 'iq' in hostname:
        return 'IQ'
    elif 'ixm' in hostname:
        return 'IXM'
    elif 'kms' in hostname:
        return 'KMS'
    elif 'mpp' in hostname:
        return 'MPP'
    elif 'oceana' in hostname:
        return 'Oceana'
    elif 'od' in hostname:
        return 'OD'
    elif 'pc5' in hostname:
        return 'PC5'
    elif 'pfsense' in hostname:
        return 'PFSense'
    elif 'reverse proxy' in hostname:
        return 'Reverse proxy'
    elif 'sic' in hostname:
        return 'SIC'
    elif 'sm' in hostname and 'smgr' not in hostname:
        return 'SM'
    elif 'smgr' in hostname:
        return 'SMGR'
    elif 'tcti' in hostname:
        return 'TCTI'
    elif 'tiger' in hostname:
        return 'Tiger'
    elif 'utility server' in hostname:
        return 'Utility server'
    elif 'vcenter' in hostname:
        return 'Vcenter'
    elif 'vyos' in hostname:
        return 'VYOS'
    elif 'wallboard' in hostname:
        return 'Wallboard'
    elif 'weblm' in hostname:
        return 'WebLM'
    elif 'wfo app' in hostname:
        return 'WFO App'
    elif 'wfo db' in hostname:
        return 'WFO DB'
    elif 'wfo fs' in hostname:
        return 'WFO FS'
    elif 'wfo rec' in hostname:
        return 'WFO Rec'
    elif 'wfo sp' in hostname:
        return 'WFO SP'
    elif 'wfo tran' in hostname:
        return 'WFO Tran'
    
    # Default to 'Unknown' if no condition is met
    return 'Unknown'

# Apply the refined mapping to the new dataframe
new_df['Configuration Item Name'] = new_df.apply(infer_configuration_item_name, axis=1)

# Save the modified dataframe back to a CSV file
output_file_path = '/mnt/data/refined_configuration_items_with_wfo_and_esxi.csv'
new_df.to_csv(output_file_path, index=False)

output_file_path
