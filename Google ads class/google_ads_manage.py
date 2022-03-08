import argparse
from datetime import datetime as dt
import sys
import uuid
from configparser import ConfigParser
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from googleads import adwords
import requests 
from google.api_core import protobuf_helpers

class Manage_Ads():
    """Class for managing Google search ads. Does not work for smart campaign ads as there is currently no API support for smart campaigns.
       Takes in a config pathway which should contain a pathway to yaml file and customer id"""
    
    
    def refresh_token(self):
        """method run right before creating a google ads client in order to insure the refresh token is authorized.
           Token authorization lasts for 60 minutes."""
        
        url = 'https://oauth2.googleapis.com/token'
        params = {'client_id': ####censored####,
                 'client_secret': ####censored####,
                 'grant_type': 'refresh_token',
                 'refresh_token': ####censored####}
        
        x = requests.post(url, data = params)
        
        if x.status_code == 200:
            return x.status_code, x.json()
        else:
            return x.status_code,x.json()
    
    
    
    
    
    def get_campaign_spent(self):
        ga_service = self.client.get_service("GoogleAdsService")
        ga_search_request = self.client.get_type("SearchGoogleAdsRequest")
        
        query = """SELECT
                metrics.cost_micros,
                campaign.name,
                campaign.status
                FROM campaign
                WHERE
                campaign.status != 'REMOVED'"""
        
        ga_search_request.customer_id = self.customer_id
        ga_search_request.query = query
        ga_search_request.page_size = 1000
        response = ga_service.search(request=ga_search_request)
        results = []
        for row in response:
            results.append([row.campaign.name, row.metrics])
        
        return results
    
    
    
    def get_campaign(self,campaign_name = 'default'):
        """gets a list of campaigns and their ids"""
        ga_service = self.client.get_service("GoogleAdsService")

        if campaign_name != 'default':
            query = """
                SELECT campaign.id, campaign.name
                FROM campaign
                WHERE campaign.name = '{}'
                ORDER BY campaign.name""".format(campaign_name)
        else:
            query = """
                SELECT campaign.id, campaign.name
                FROM campaign
                ORDER BY campaign.name"""
    
        # Issues a search request using streaming.
        response = ga_service.search_stream(customer_id=self.customer_id, query=query)
        results = []
        for batch in response:
            for row in batch.results:
                results.append([row.campaign.id,row.campaign.name])
        
        return results
    
    
    
    def get_budgets(self,budget_ids=None):
        ga_service = self.client.get_service("GoogleAdsService")
        search_request = self.client.get_type("SearchGoogleAdsRequest")
        
        
        query ="""
                    SELECT campaign_budget.amount_micros, campaign_budget.id,
                    campaign_budget.status, campaign_budget.name
                    FROM campaign_budget
                    """            
        
        
        if budget_ids:
            query += """ WHERE campaign_budget.id IN ({})""".format("".join(str(budget_ids)).replace("[","").replace("]",""))
        
        search_request.customer_id = self.customer_id
        search_request.query = query
        #the max number of results to return
        search_request.page_size = 1000
    
        results = ga_service.search(request=search_request)
        return results

    
    
    
    
    
    def _ad_text_assets_to_strs(self,assets):
        """Converts a list of AdTextAssets to a list of user-friendly strings."""
        s = []
        for asset in assets:
            s.append(f"\t {asset.text} pinned to {asset.pinned_field.name}")
        return s
    
    
    
    def get_ad_id(self, ad_group_id=None):
        """ Gets list of ads and their ids, as well as description and headlines to help identify ads"""
        ga_service = self.client.get_service("GoogleAdsService")
        ga_search_request = self.client.get_type("SearchGoogleAdsRequest")
    
        query = """
                    SELECT ad_group.id,
                    ad_group.name,
                    ad_group_ad.ad.id,
                    ad_group_ad.ad.name,
                    ad_group_ad.status,
                    ad_group_ad.ad.responsive_search_ad.headlines,
                    ad_group_ad.ad.responsive_search_ad.descriptions
                    FROM ad_group_ad
                    WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status != "REMOVED"
                    """
    
        if ad_group_id:
            query += f" AND ad_group.id = {ad_group_id}"
        
        ga_search_request.customer_id = self.customer_id
        ga_search_request.query = query
        ##max number of results back
        ga_search_request.page_size = 1000
        results = ga_service.search(request=ga_search_request)
        
        one_found = False
        for row in results:
            one_found = True
            ad = row.ad_group_ad.ad
            print(
                "Responsive search ad with resource name "
                f'"{ad.resource_name}", status {row.ad_group_ad.status.name},'
                f' group id "{row.ad_group.id}", ad group name "{row.ad_group.name}", ad name "{ad.name}"'
                "was found."
            )
        
            headlines = "\n".join(self._ad_text_assets_to_strs(ad.responsive_search_ad.headlines))
            descriptions = "\n".join(self._ad_text_assets_to_strs(ad.responsive_search_ad.descriptions))
            print(f"Headlines:\n{headlines}\nDescriptions:\n{descriptions}\n")

        if not one_found:
            print("No responsive search ads were found.")
            
        return results
    
    
    
     def get_ad_groups(self,campaign_id = None):
        
        """Returns data in this form for all ad groups in all campaigns (if no campaign_id is supplied)
           SearchPager<results {
           campaign {
             resource_name: "customers/7506231546/campaigns/11810953335"
             name: "Richmond Hill Homes For Sale - Under 800k"
             id: 11810953335
             }
           ad_group {
             resource_name: "customers/7506231546/adGroups/113664221686"
             id: 113664221686
             name: "Smart Campaign Managed AdGroup"
             }
           }"""
        
        ga_service = self.client.get_service("GoogleAdsService")
        search_request = self.client.get_type("SearchGoogleAdsRequest")
        
        query = """SELECT
                      campaign.id,
                      campaign.name,
                      ad_group.id,
                      ad_group.name
                   FROM ad_group"""
        if campaign_id:
            query += f" WHERE campaign.id = {campaign_id}"
        
        
        search_request.customer_id = self.customer_id
        search_request.query = query
        #the max number of results to return
        search_request.page_size = 1000
        
        
        results = ga_service.search(request=search_request)

        for row in results:
            print(
                f"Ad group with ID {row.ad_group.id} and name "
                f'"{row.ad_group.name}" was found in campaign {row.campaign.name} with '
                f"ID {row.campaign.id}."
            )
        return results
    
    
    def get_keywords(self, ad_group_id=None):
        
        ga_service = self.client.get_service("GoogleAdsService")
        search_request = self.client.get_type("SearchGoogleAdsRequest")
        
        query = """
            SELECT
              ad_group.id,
              ad_group_criterion.type,
              ad_group_criterion.criterion_id,
              ad_group_criterion.keyword.text,
              ad_group_criterion.keyword.match_type
            FROM ad_group_criterion
            WHERE ad_group_criterion.type = KEYWORD"""
        
        if ad_group_id:
            query += f" AND ad_group.id = {ad_group_id}"

        search_request.customer_id = self.customer_id
        search_request.query = query
        #the max number of results to return
        search_request.page_size = 1000
        
        
        results = ga_service.search(request=search_request)

        for row in results:
            ad_group = row.ad_group
            ad_group_criterion = row.ad_group_criterion
            keyword = row.ad_group_criterion.keyword
    
            print(
                f'Keyword with text "{keyword.text}", match type '
                f"{keyword.match_type}, criteria type "
                f"{ad_group_criterion.type_}, and ID "
                f"{ad_group_criterion.criterion_id} was found in ad group "
                f"with ID {ad_group.id}."
            )
        
        return results
    
    
    
    
    def pause_campaign(self,campaign_id):
        """ Pauses the campaign of the given id"""
        ##initialize required services
        campaign_service = self.client.get_service("CampaignService")
        campaign_operation = self.client.get_type("CampaignOperation")
        #set operation type to update
        campaign = campaign_operation.update
        #specify path of resource
        campaign.resource_name = campaign_service.campaign_path(
            self.customer_id, campaign_id
        )
        
        #sets campaign status to paused
        campaign.status = "PAUSED"
    
        # Retrieve a FieldMask for the fields configured in the campaign.
        self.client.copy_from(
            campaign_operation.update_mask,
            protobuf_helpers.field_mask(None, campaign._pb),
        )
        
        # Executes changes
        try:
            campaign_response = campaign_service.mutate_campaigns(
            customer_id=self.customer_id, operations=[campaign_operation]
        )
            
        except GoogleAdsException as ex:
            _handle_googleads_exception(ex)      
            #
        print(f"paused campaign {campaign_response.results[0].resource_name}.")
        

        
        
        
    def resume_campaign(self,campaign_id):
        """ Resumes paused campaign"""
        ##initialize required services
        campaign_service = self.client.get_service("CampaignService")
        campaign_operation = self.client.get_type("CampaignOperation")
        #set operation type to update
        campaign = campaign_operation.update
        #specify path of resource
        campaign.resource_name = campaign_service.campaign_path(
            self.customer_id, campaign_id
        )
         #sets campaign status to enabled
        campaign.status = "ENABLED"
    
        # Retrieve a FieldMask for the fields configured in the campaign.
        self.client.copy_from(
            campaign_operation.update_mask,
            protobuf_helpers.field_mask(None, campaign._pb),
        )
        
        # Executes changes
        try:
            campaign_response = campaign_service.mutate_campaigns(
            customer_id=self.customer_id, operations=[campaign_operation]
        )
            
        except GoogleAdsException as ex:
            _handle_googleads_exception(ex)      
            #
        print(f"campaign resumed{campaign_response.results[0].resource_name}.")
       
    
    
    
       
    def create_budget(self,budget_name,budget_amount):
        """ Creates a budget plan based off of the name and amount
            Amount is in micros i.e. 1,000,000micros = $1.00"""
        
        ##initialize google services (one for setting up budget, one for creating the actual ad)
        campaign_budget_service = self.client.get_service("CampaignBudgetService")
        campaign_service = self.client.get_service("CampaignService")
        
        
        ## Create a budget, which can be shared by multiple campaigns.
        campaign_budget_operation = self.client.get_type("CampaignBudgetOperation")
        campaign_budget = campaign_budget_operation.create

        campaign_budget.name = f"{budget_name}"
        campaign_budget.delivery_method = self.client.get_type(
            "BudgetDeliveryMethodEnum"
        ).BudgetDeliveryMethod.STANDARD
        campaign_budget.amount_micros = budget_amount
        
        
        # Add budget.
        try:
            campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
                customer_id=self.customer_id, operations=[campaign_budget_operation]
            )
            return campaign_budget_response.results[0]
        except GoogleAdsException as ex:
            _handle_googleads_exception(ex)         
            

     def change_budget(self,budget_path,new_micro_amount):
        budget_service = self.client.get_service("CampaignBudgetService")
        campaign_budget_operation = self.client.get_type("CampaignBudgetOperation")
        
        camapign_budget = campaign_budget_operation.update
        camapign_budget.resource_name = budget_path
        camapign_budget.amount_micros = new_micro_amount
        
        self.client.copy_from(
            campaign_budget_operation.update_mask,
            protobuf_helpers.field_mask(None, camapign_budget._pb),
        )
        
        
        campaign_budget_response = budget_service.mutate_campaign_budgets(
                customer_id=self.customer_id, operations=[campaign_budget_operation]
            )        
            
    
    
    
    def remove_budget(self,budget_id):
        budget_service = self.client.get_service("CampaignBudgetService")
        budget_operation = self.client.get_type("CampaignBudgetOperation")
        search_request = self.client.get_type("SearchGoogleAdsRequest")
        ga_service = self.client.get_service("GoogleAdsService")
        
        ### checks to make sure the budget 
        query ="""SELECT campaign.id,campaign.name,campaign.status
                    FROM campaign_budget
                    WHERE campaign_budget.id = {}
                    AND campaign.status != 'REMOVED'""".format(budget_id)
            
        search_request.customer_id = self.customer_id
        search_request.query = query
        #the max number of results to return
        search_request.page_size = 1000
    
        results = ga_service.search(request=search_request)
        resource_name = ''
        for row in results:
            print('Active campaigns are still using this budget plan')
            return results
        resource_name = 'customers/{}/campaignBudgets/{}'.format(self.customer_id,budget_id)
        
        
        budget_operation.remove = resource_name
        
        budget_remove_response = budget_service.mutate_campaign_budgets(
            customer_id = self.customer_id, operations = [budget_operation])
        
        print('removed ',resource_name)
    
    
    
    def set_budget(self,campaign_id,budget_pathway):
        """sets the given campaign to the budget plan with the given name. Must be in this format
        
            'customers/{customer_id}/campaignBudgets/{campaign_budget_id}'
            
            e.g. customers/7506231546/campaignBudgets/9385497260
            (7506231546 is farid's customer id, 9385497260 is the id for 'qumars-budget' plan)
           
            return from create_budget method is already set to this"""
        ##initialize google services (one for setting up budget, one for creating the actual ad)
        campaign_service = self.client.get_service("CampaignService")
        campaign_operation = self.client.get_type("CampaignOperation")
        campaign = campaign_operation.update
        campaign.resource_name = campaign_service.campaign_path(
            self.customer_id, campaign_id
        )
        
        campaign.campaign_budget = str(budget_name)
    
        # Retrieve a FieldMask for the fields configured in the campaign.
        self.client.copy_from(
            campaign_operation.update_mask,
            protobuf_helpers.field_mask(None, campaign._pb),
        )
        #
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=self.customer_id, operations=[campaign_operation]
        )
        #
        print(f"campaign budget set to {budget_name} for {campaign_response.results[0].resource_name}.")
            
            
     def _create_location_op(self,campaign_id):
        """20121 is the location ID for Ontario"""
        campaign_service = self.client.get_service("CampaignService")
        geo_target_constant_service = self.client.get_service("GeoTargetConstantService")
        campaign_criterion_operation = self.client.get_type("CampaignCriterionOperation")
        campaign_criterion_service = self.client.get_service("CampaignCriterionService")
        
        
        campaign_criterion = campaign_criterion_operation.create
        campaign_criterion.campaign = campaign_service.campaign_path(
            self.customer_id, campaign_id
        )

        
        campaign_criterion.location.geo_target_constant = (
            geo_target_constant_service.geo_target_constant_path('20121')
        )
        
        location_response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=self.customer_id, operations=[campaign_criterion_operation])

        return campaign_criterion_operation
    
    
    
    
    def create_campaign(self,campaign_name,budget_name,target_cpa):
        """budget_name must be in this format
           'customers/{customer_id}/campaignBudgets/{campaign_budget_id}'
           
           e.g. customers/7506231526/campaignBudgets/9385497262
          
           
        """
        
        campaign_service = self.client.get_service("CampaignService")
        
        # Create campaign.
        campaign_operation = self.client.get_type("CampaignOperation")
        campaign = campaign_operation.create
        ###campaign names MUST be unique per account, so either enforce a check to make sure they arent using that name already, or have this uuid generated and attached to campaigns
        campaign.name = f" {uuid.uuid4(){campaign_name}}"
        campaign.advertising_channel_type = (self.client.enums.AdvertisingChannelTypeEnum.SEARCH)
        
        #Pause campaign so it does not start immediately
        campaign.status = self.client.enums.CampaignStatusEnum.PAUSED
        
        #set bidding strategy and budget
        campaign.maximize_conversions.target_cpa = target_cpa
        campaign.campaign_budget = str(budget_name)
        
        #### Set the campaign network options.
        
        #Ads will be served with Google.com search results.
        campaign.network_settings.target_google_search = True
        #Ads will be served on partner sites in the Google Search Network (requires GOOGLE_SEARCH).
        campaign.network_settings.target_search_network = True
        #Ads will be served on specified placements in the Google Display Network. Placements are specified using Placement criteria.
        campaign.network_settings.target_content_network = False
        # targetPartnerSearchNetwork is available only to select partners and should be set to "false" or "null" for everyone else.
        campaign.network_settings.target_partner_search_network = False
        
        
        
        try:
            campaign_response = campaign_service.mutate_campaigns(
                customer_id=self.customer_id, operations=[campaign_operation]
            )
            print(f"Created campaign {campaign_response.results[0].resource_name}.")
        except GoogleAdsException as ex:
            _handle_googleads_exception(ex)
        
            
        
    def remove_campaign(self,campaign_id):
        """ Deletes the campaign with the given id"""
        campaign_service = self.client.get_service("CampaignService")
        campaign_operation = self.client.get_type("CampaignOperation")
        search_request = self.client.get_type("SearchGoogleAdsRequest")
        ga_service = self.client.get_service("GoogleAdsService")
        
        
        ###get budget id of campaign
        query ="""SELECT campaign_budget.id, campaign.id
                    FROM campaign_budget
                    WHERE campaign.id = {}""".format(campaign_id)
        
        search_request.customer_id = self.customer_id
        search_request.query = query
        #the max number of results to return
        search_request.page_size = 1000
    
        results = ga_service.search(request=search_request)
        one_found = False
        budget_id = ''
        for row in results:
            one_found = True
            budget_id = row.campaign.id
        
        
    
        resource_name = campaign_service.campaign_path(self.customer_id, campaign_id)
        campaign_operation.remove = resource_name
    
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=self.customer_id, operations=[campaign_operation]
        )
    
        print(f"Removed campaign {campaign_response.results[0].resource_name}.")
        
        if one_found == True:
            print(budget_id)
            self.remove_budget(budget_id)
        else:
            print('no budget id was found for the given campaign')
        
    
    def create_ad_group(self,ad_group_name,campaign_id:str):
        """Creates an ad group inside a campaign where ads for that campaign can be stored
        
            Campaign_id must be given as a string"""
        
        ad_group_service = self.client.get_service("AdGroupService")
        campaign_service = self.client.get_service("CampaignService")
        
        
        # Create ad group.
        ad_group_operation = self.client.get_type("AdGroupOperation")
        ad_group = ad_group_operation.create
        ad_group.name = f"{ad_group_name} | {uuid.uuid4()}"
        ad_group.status = self.client.enums.AdGroupStatusEnum.ENABLED
        ad_group.campaign = campaign_service.campaign_path(self.customer_id, campaign_id)
        ad_group.type_ = self.client.enums.AdGroupTypeEnum.SEARCH_STANDARD
        ad_group.cpc_bid_micros = 5000000
        #can set a budget for an adgroup so all ads in group will inherit that budget
        #ad_group.cpc_bid_micros = 10000000
        
        
        # Add the ad group.
        ad_group_response = ad_group_service.mutate_ad_groups(
            customer_id=self.customer_id, operations=[ad_group_operation]
        )
        print(f"Created ad group {ad_group_response.results[0].resource_name}.")
        
        
    def remove_ad_group(self,ad_group_id):
        ad_group_service = self.client.get_service("AdGroupService")
        ad_group_operation = self.client.get_type("AdGroupOperation")
    
        resource_name = ad_group_service.ad_group_path(self.customer_id, ad_group_id)
        ad_group_operation.remove = resource_name
    
        ad_group_response = ad_group_service.mutate_ad_groups(
            customer_id=self.customer_id, operations=[ad_group_operation]
        )
    
        print(f"Removed ad group {ad_group_response.results[0].resource_name}.")
    
    
    
    def add_keywords(self,ad_group_id,keyword):
        """Adds keywords to the ad group"""
        ad_group_service = self.client.get_service("AdGroupService")
        ad_group_criterion_service = self.client.get_service("AdGroupCriterionService")
        
        
        # Create keyword.
        ad_group_criterion_operation = self.client.get_type("AdGroupCriterionOperation")
        ad_group_criterion = ad_group_criterion_operation.create
        ad_group_criterion.ad_group = ad_group_service.ad_group_path(self.customer_id, ad_group_id)
        ad_group_criterion.status = self.client.enums.AdGroupCriterionStatusEnum.ENABLED
        
        

        ad_group_criterion.keyword.text = keyword
        print(ad_group_criterion.keyword.text)
        ad_group_criterion.keyword.match_type = (self.client.enums.KeywordMatchTypeEnum.BROAD)
        #ad_group_criterion.final_urls.append(landing_page)
        
        # Add keyword
        ad_group_criterion_response = (
        ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=self.customer_id,
            operations=[ad_group_criterion_operation],
        )
    )

        print(
            "Created keyword "
            f"{ad_group_criterion_response.results[0].resource_name}."
        )
        
        
    def _create_ad_text_asset(self,text, pinned_field=None):
        """Create an AdTextAsset."""
        ad_text_asset = self.client.get_type("AdTextAsset")
        ad_text_asset.text = text
        if pinned_field:
            ad_text_asset.pinned_field = pinned_field
        return ad_text_asset
    
    
     def remove_keywords(self,ad_group_id):
        
        
        agc_service = self.client.get_service("AdGroupCriterionService")
        agc_operation = self.client.get_type("AdGroupCriterionOperation")
        search_request = self.client.get_type("SearchGoogleAdsRequest")
        ga_service = self.client.get_service("GoogleAdsService")
        
        query = """
            SELECT
              ad_group_criterion.criterion_id
            FROM ad_group_criterion
            WHERE ad_group.id = {}
            AND ad_group_criterion.status != 'REMOVED' """.format(ad_group_id)
        
        if ad_group_id:
            query += f" AND ad_group.id = {ad_group_id}"

        search_request.customer_id = self.customer_id
        search_request.query = query
        #the max number of results to return
        search_request.page_size = 1000
        
        
        results = ga_service.search(request=search_request)
        
        one_found = False
        for row in results:   
            
            resource_name = row.ad_group_criterion.resource_name
            agc_operation.remove = resource_name
            
        
            agc_response = agc_service.mutate_ad_group_criteria(
                customer_id=self.customer_id, operations=[agc_operation]
            )
            one_found = True
        
        if one_found:
            print(f"Removed keywords")
        else:
            print(f"no keywords to remove")
    
    
    
    
    def add_ad(self,ad_group_id,ad_url,headlines,descriptions,display_path1 = None,display_path2 = None):
        ad_group_ad_service = self.client.get_service("AdGroupAdService")
        ad_group_service = self.client.get_service("AdGroupService")
        
        

        # Create the ad group ad.
        ad_group_ad_operation = self.client.get_type("AdGroupAdOperation")
        ad_group_ad = ad_group_ad_operation.create
        ad_group_ad.status = self.client.enums.AdGroupAdStatusEnum.PAUSED
        ad_group_ad.ad_group = ad_group_service.ad_group_path(self.customer_id, ad_group_id)
        
        ad_group_ad.ad.final_urls.append(ad_url)
        
        
        
        #Set a pinning to always choose this asset for HEADLINE_1. Pinning is
        # optional; if no pinning is set, then headlines and descriptions will be
        # rotated and the ones that perform best will be used more often.
        #served_asset_enum = client.enums.ServedAssetFieldTypeEnum.HEADLINE_1
       # pinned_headline = _create_ad_text_asset(self.client, f"Cruise to Mars #{str(uuid4())[:8]}", served_asset_enum)
        
        # Create Headlines (must have 3, max 15)
        if len(headlines)>= 3 and len(headlines) <=15:
            for h in headlines:
                ad_group_ad.ad.responsive_search_ad.headlines.extend([
                    #pinned_headline,
                    self._create_ad_text_asset(h),
                    #self._create_ad_text_asset("Cheap homes in Oshawa"),
                    #self._create_ad_text_asset("Detached Homes in Oshawa"),
                ])
        
        
        # Create Description (must have 2, max 4)
        if len(descriptions)>= 2 and len(descriptions) <=4:
            for d in descriptions:
                ad_group_ad.ad.responsive_search_ad.descriptions.extend([
                        self._create_ad_text_asset(d),
                        #self._create_ad_text_asset("MLS updated hourly"),
                    ])
        
        #display path of url for ad
        if display_path1 != None:
            ad_group_ad.ad.responsive_search_ad.path1 = display_path1
            
        if display_path2 != None:
            ad_group_ad.ad.responsive_search_ad.path2 = display_path2
        
        
        # Send a request to the server to add a responsive search ad.
        ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=self.customer_id, operations=[ad_group_ad_operation]
        )
    
        for result in ad_group_ad_response.results:
            print(
                f"Created responsive search ad with resource name "
                f'"{result.resource_name}".'
                )
    
    
     def remove_ad(self,ad_group_id, ad_id):
        ad_group_ad_service = self.client.get_service("AdGroupAdService")
        ad_group_ad_operation = self.client.get_type("AdGroupAdOperation")

        resource_name = ad_group_ad_service.ad_group_ad_path(
            self.customer_id, ad_group_id, ad_id
        )
        ad_group_ad_operation.remove = resource_name

        ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=self.customer_id, operations=[ad_group_ad_operation]
        )

        print(
            f"Removed ad group ad {ad_group_ad_response.results[0].resource_name}."
        )
    
    
    
    
    def __init__(self,config_pathway):
        """ Initializes class, creates a google ads client as well as stores the custome id for use in other functions"""
        ##read configuration file
        read = ConfigParser()
        read.read('ad_create_config.ini')
        self.yaml_path= read.get('Google API','yaml_path')
        self.customer_id= read.get('Google API','customer_id')
        
        
        status,data = self.refresh_token()
        if status == 200:    
            self.client = GoogleAdsClient.load_from_storage(self.yaml_path)
            print(status,'\n',data)
        else:
            try:
                print('error code: ',status)
                print('error type: ',data['error'])
                print('error description: ',data['error_description'])
            except Exception as e:
                print(e)
        
        ##create instance of google ads client
        self.client = GoogleAdsClient.load_from_storage(self.yaml_path)

