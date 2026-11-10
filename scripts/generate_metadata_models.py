"""Generate our typed, bounded metadata projection from an explicit wire-field table.

Wire numbers/types are interoperability facts checked against connector 2.5.0.
This is not a copy or a generator for its full User/protocol schema.
"""
from pathlib import Path
import json

ROOT=Path(__file__).resolve().parents[1]
# tag, Rust/API field name, scalar type or nested type, repeated/map marker
MODELS={
 'ImageContent':[(1,'name','string'),(2,'font_color','string'),(3,'level','i64')],
 'Image':[(1,'url_list','url','many8'),(2,'uri','string'),(3,'height','i32'),(4,'width','i32'),(5,'avg_color','string'),(6,'image_type','i32'),(7,'open_web_url','string'),(8,'content','ImageContent'),(9,'is_animated','bool')],
 'FollowInfo':[(1,'following_count','i64'),(2,'follower_count','i64'),(3,'follow_status','i64'),(4,'push_status','i64')],
 'WireUser':[(1,'id','u64'),(3,'nickname','string'),(9,'avatar_thumb','Image'),(10,'avatar_medium','Image'),(11,'avatar_large','Image'),(12,'verified','bool'),(22,'follow_info','FollowInfo'),(38,'display_id','string'),(46,'sec_uid','string'),(1028,'id_str','string')],
 'TextFormat':[(1,'color','string'),(2,'bold','bool'),(3,'italic','bool'),(4,'weight','i32'),(5,'italic_angle','i32'),(6,'font_size','i32'),(7,'use_heigh_light_color','bool'),(8,'use_remote_clor','bool')],
 'Text':[(1,'key','string'),(2,'default_pattern','string'),(3,'default_format','TextFormat'),(4,'pieces','TextPiece','many32')],
 'TextPiece':[(1,'r#type','i32'),(2,'format','TextFormat'),(11,'string_value','string'),(21,'user_value','TextUser'),(22,'gift_value','TextGift'),(23,'heart_value','TextHeart'),(24,'pattern_ref_value','TextPattern'),(25,'image_value','TextImage')],
 'TextUser':[(1,'user','WireUser'),(2,'with_colon','bool')],
 'TextGift':[(1,'gift_id','i64'),(2,'name_ref','PatternRef'),(3,'show_type','i32'),(4,'color_id','i64')],
 'TextHeart':[(1,'color','string')],
 'TextPattern':[(1,'key','string'),(2,'default_pattern','string')],
 'TextImage':[(1,'image','Image')],
 'PatternRef':[(1,'key','string'),(2,'default_pattern','string'),(3,'key_groups','string','map16')],
 'StringEntry':[(1,'key','string'),(2,'value','string')],
 'MessageId':[(1,'primary_id','string'),(2,'message_scene','string')],
 'MessageSei':[(1,'unique_id','MessageId'),(2,'timestamp','i64')],
 'MessageCommon':[(1,'method','string'),(2,'msg_id','u64'),(3,'room_id','u64'),(4,'create_time','i64'),(5,'monitor','i32'),(6,'is_show_msg','bool'),(7,'describe','string'),(8,'display_text','Text'),(9,'fold_type','i64'),(10,'anchor_fold_type','i64'),(11,'priority_score','i64'),(12,'log_id','string'),(13,'msg_process_filter_k','string'),(14,'msg_process_filter_v','string'),(15,'from_idc','string'),(16,'to_idc','string'),(17,'filter_msg_tags','string','many32'),(18,'sei','MessageSei'),(19,'depend_root_id','MessageId'),(20,'depend_id','MessageId'),(21,'anchor_priority_score','i64'),(22,'room_message_heat_level','i64'),(23,'fold_type_for_web','i64'),(24,'anchor_fold_type_for_web','i64'),(25,'client_send_time','i64'),(26,'dispatch_strategy','i32')],
 'SpecifiedText':[(1,'uid','i64'),(2,'display_text','Text')],
 'LikeEffect':[(1,'version','i64'),(2,'effect_cnt','i64'),(3,'effect_interval_ms','i64'),(4,'level','i64')],
 'TagItem':[(1,'tag_type','i32'),(2,'tag_text','Text'),(3,'tag_value','i64'),(4,'display_location','i32'),(5,'event_tracking_fields','string','map16'),(6,'biz_name','string'),(10,'disable_count_frequency_control','bool'),(11,'disable_crm_click','bool')],
 'ActionButton':[(1,'button_type','i32'),(2,'button_active_text','Text'),(3,'button_inactive_text','Text'),(4,'button_schema','string'),(5,'enable_button_inactive_text','bool')],
 'Topic':[(1,'topic_action_type','i32'),(2,'topic_text','Text'),(3,'topic_tips','Text'),(4,'has_button','bool'),(5,'action_button','ActionButton'),(6,'tag_type','i32'),(7,'display_location','i32'),(8,'event_tracking_fields','string','map16'),(9,'biz_name','string'),(10,'disable_count_frequency_control','bool')],
 'FrequencyControl':[(1,'frequency_control_key','string'),(2,'window_size','i64'),(3,'limit_in_window','i64')],
 'CreatorSuccess':[(1,'tags','TagItem','many32'),(2,'topic','Topic'),(3,'topic_list','Topic','many32'),(4,'frequency_control','FrequencyControl')],
 'UserMetrics':[(1,'r#type','i32'),(2,'metrics_value','string')],
 'PortraitTag':[(1,'tag_id','string'),(2,'priority','i64'),(3,'show_value','string'),(4,'show_args','string')],
 'PortraitInfo':[(1,'user_metrics','UserMetrics','many32'),(2,'portrait_tag','PortraitTag','many32')],
 'UserInteraction':[(1,'like_cnt','i64'),(2,'comment_cnt','i64'),(3,'share_cnt','i64')],
 'MessageDisplay':[(1,'duration_type','i32'),(2,'show_duration_ms','i64'),(3,'incr_duration_ms','i64'),(4,'stay_style','i32')],
 'OperationState':[(1,'scene','string'),(2,'icon','Image'),(3,'text','Text'),(4,'schema','string'),(5,'animation_style','i64'),(6,'duration','i64')],
 'OperationInfo':[(1,'initial_state','OperationState'),(2,'success_state','OperationState'),(3,'failure_state','OperationState')],
 'PublicArea':[(1,'scroll_gap_count','i64'),(2,'anchor_scroll_gap_count','i64'),(3,'release_to_scroll_area','bool'),(4,'anchor_release_to_scroll_area','bool'),(5,'is_anchor_marked','bool'),(6,'creator_success_info','CreatorSuccess'),(7,'portrait_info','PortraitInfo'),(8,'user_interaction_info','UserInteraction'),(9,'admin_fold_type','i64'),(10,'display_info','MessageDisplay'),(11,'hide_above_scroll_area','bool'),(12,'operation_info','OperationInfo'),(13,'dynamic_display_text','Text')],
 # count/total are already in Event.data, and the sender is Event.user.
 'LikeDetails':[(1,'common','MessageCommon'),(4,'color','i32'),(6,'icon','string'),(7,'icons','Image','many16'),(8,'specified_display_text','SpecifiedText','many32'),(9,'effect_cnt','i64'),(10,'like_effect','LikeEffect','many32'),(11,'public_area_message_common','PublicArea'),(12,'room_message_heat_level','i64')],
}


def main():
    output=['// Generated by scripts/generate_metadata_models.py; edit the explicit table there.',
            'use prost::Message;', 'use serde::Serialize;', 'use super::{decimal, Rule, Shape};']
    scalars={'i32':'int32','i64':'int64','u64':'uint64','bool':'bool','string':'string','url':'string'}
    for name,fields in MODELS.items():
        output += ['#[derive(Clone, PartialEq, Message, Serialize)]',f'pub struct {name} {{']
        for tag,field,kind,*marker in fields:
            repeated=marker[0] if marker else ''
            if repeated.startswith('map'):
                attribute='btree_map = "string, string"'
                rust='std::collections::BTreeMap<String, String>'
            else:
                atom='String' if kind in ('string','url') else kind
                attribute=scalars.get(kind,'message')
                if repeated:
                    attribute+=', repeated';rust=f'Vec<{atom}>'
                elif kind not in scalars:
                    attribute+=', optional, boxed';rust=f'Option<Box<{atom}>>'
                else:rust=atom
            output += [f'    #[prost({attribute}, tag = "{tag}")]']
            if kind in ('u64','i64'):output+=['    #[serde(serialize_with = "decimal")]']
            output += [f'    pub {field}: {rust},']
        output += ['}',f'impl {name} {{','    pub(super) fn rule(tag: u32) -> Rule {','        match tag {']
        for tag,field,kind,*marker in fields:
            marker=marker[0] if marker else ''
            max_count=int(marker.removeprefix('map').removeprefix('many')) if marker else 0
            if marker.startswith('map'):shape='Shape::Nested(StringEntry::rule)'
            elif kind not in scalars:shape=f'Shape::Nested({kind}::rule)'
            elif kind in ('string','url'):shape=f'Shape::Text({2048 if kind=="url" else 4096})'
            else:shape='Shape::Scalar'
            output += [f'            {tag} => Rule {{ shape: {shape}, max_count: {max_count} }},']
        output += ['            _ => Rule::IGNORE,','        }','    }','    pub fn within_limits(&self) -> bool {']
        conditions=[]
        for tag,field,kind,*marker in fields:
            marker=marker[0] if marker else ''
            if marker.startswith('map'):
                conditions.append(f'self.{field}.len() <= {marker[3:]} && self.{field}.iter().all(|(k,v)| k.len() <= 4096 && v.len() <= 4096)')
            elif marker:
                check='v.within_limits()' if kind not in scalars else f'v.len() <= {2048 if kind=="url" else 4096}'
                conditions.append(f'self.{field}.len() <= {marker[4:]} && self.{field}.iter().all(|v| {check})')
            elif kind not in scalars:conditions.append(f'self.{field}.as_ref().is_none_or(|v| v.within_limits())')
            elif kind in ('string','url'):conditions.append(f'self.{field}.len() <= {2048 if kind=="url" else 4096}')
        output += ['        '+ (' &&\n        '.join(conditions) if conditions else 'true'), '    }','}']
    (ROOT/'src/metadata/models.rs').write_text('\n'.join(output)+'\n',encoding='utf-8')
    (ROOT/'benchmarks/connector/metadata-schema.json').write_text(json.dumps(MODELS,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':main()
