export interface OtsovProjectArtifact { schema:'soty.otsov-project-artifact.v1';appId:string;projectId:string;branch:string;revision:number;invocationId:string }
export interface EditorReadback {projectId:string;branch:string;revision:number;selectedProjectId:string;online:true}
export const EDITOR_HANDOFF_SCHEMA:string;
export function exact(v:unknown,names:string[]):boolean;
export function projectId(v:unknown):boolean;
export function branchId(v:unknown):boolean;
export function requestId(v:unknown):boolean;
export function validArtifact(v:unknown):v is OtsovProjectArtifact;
export function validReadback(v:unknown,a:OtsovProjectArtifact):v is EditorReadback;
export function copyArtifact(v:unknown):OtsovProjectArtifact;
