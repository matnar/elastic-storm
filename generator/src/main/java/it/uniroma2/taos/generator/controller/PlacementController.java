package it.uniroma2.taos.generator.controller;

import it.uniroma2.taos.generator.service.PlacementService;
import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
@Controller
@RequestMapping("/placement")
public class PlacementController {

	@Resource(name = "placementService")
	PlacementService ps;

	@RequestMapping(method = RequestMethod.POST)
	@ResponseBody
	public PlacementResponse printWelcome(@RequestBody PlacementRequest rq) {
		return ps.getPlacement(rq);

	}

}