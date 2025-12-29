package mx.baz.conciliaciones.operaciones.dto;


import lombok.Data;

@Data
public class PathInfo {
	private String job;
	private String ip;
	private String path;
	private String file;
	private String domain;
	private String user;
	private String password;
}
